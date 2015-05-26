﻿using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using RedisJiggeryPokery.Contracts;
using StackExchange.Redis;

namespace RedisJiggeryPokery
{
    // TODO: Investigate multi endpoint stuff. May prove useful.

    public class RedisDataProvider<T> : IRedisDataProvider<T>
    {
        private string _connectionString;
        private int _databaseIndex;
        private ConnectionMultiplexer _redisConnectionMultiPlexer;

        /// <summary>
        /// Constructor for the data provider.
        /// </summary>
        /// <param name="connectionString">
        /// Connection string to the target Redis datastore.
        /// Putting in null or empty string will result in this object
        /// connecting to localhost.
        /// </param>
        /// <param name="targetConnectionMultiplexer">
        /// Connection/Multiplexer to Redis data store. 
        /// This is here jus in case you want to use your own ConnectionMultiplexer.
        /// </param>
        /// <param name="targetDatabaseIndex">
        /// Target Redis Database Index.
        /// Defaults to 0 as per StackExchange.Redis API defaults.
        /// </param>
        public RedisDataProvider(
            string connectionString, 
            ConnectionMultiplexer targetConnectionMultiplexer = null,
            int targetDatabaseIndex = 0)
        {
            _connectionString = connectionString;
            _redisConnectionMultiPlexer = targetConnectionMultiplexer;
            _databaseIndex = targetDatabaseIndex;
        }

        public string ConnectionString
        {
            get
            {
                if (string.IsNullOrEmpty(_connectionString))
                {
                    _connectionString = "localhost";
                }

                return _connectionString;
            }
            set
            {
                _connectionString = value;

                RedisConnectionMultiPlexer = ConnectionMultiplexer.Connect(_connectionString);
            }
        }

        public int DatabaseIndex
        {
            get
            {
                if (_databaseIndex < 0)
                {
                    _databaseIndex = 0;
                }

                return _databaseIndex;
            }
            set { _databaseIndex = value; }
        }

        private ConnectionMultiplexer RedisConnectionMultiPlexer
        {
            get
            {
                if (_redisConnectionMultiPlexer == null)
                {
                    ConnectionMultiplexer.Connect(ConnectionString);
                }

                return _redisConnectionMultiPlexer;
            }
            set { _redisConnectionMultiPlexer = value; }
        }

        public IList<T> GetAllKeyValuePair(int dbIndex = 0)
        {
            var targetDatabase = RedisConnectionMultiPlexer.GetDatabase(dbIndex);

            var targetType = typeof(T);
            var targetTypeNameString = targetType.Name;
            var keysInSet = targetDatabase.SetMembers(targetTypeNameString).ToList();

            return keysInSet.Count > 0 ? 
                GetValuesBasedOnSetValues(targetDatabase, keysInSet) : 
                GetAllValuesByWildcard(RedisConnectionMultiPlexer, targetDatabase, DatabaseIndex);
        }

        private static IList<T> GetValuesBasedOnSetValues(IDatabase targetDatabase, IList<RedisValue> keysRetrievedFromSet)
        {
            if (targetDatabase == null) throw new ArgumentNullException("targetDatabase");
            if (keysRetrievedFromSet == null) throw new ArgumentNullException("keysRetrievedFromSet");

            // TODO : Need to examine if the parallel stuff actually has benefits.
            var redisKeys = keysRetrievedFromSet
                    .Select(x => (RedisKey)((string)x))
                    .AsParallel()
                    .ToArray();

            var retrievedItems = targetDatabase.StringGet(redisKeys).ToList();
            var castedRetrievedItems = retrievedItems
                .Select(x => JsonConvert.DeserializeObject<T>(x))
                .AsParallel()
                .ToList();

            return castedRetrievedItems;
        }

        // TODO: Evaluate if this is still a valid approach. Looks stupid.
        private static IList<T> GetAllValuesByWildcard(
            ConnectionMultiplexer redisConnectionMultiplexer,
            IDatabase targetDatabase, 
            int databaseIndex)
        {
            if (redisConnectionMultiplexer == null) throw new ArgumentNullException("redisConnectionMultiplexer");
            if (targetDatabase == null) throw new ArgumentNullException("targetDatabase");
            if (databaseIndex < 0) databaseIndex = 0;

            var endPoints = redisConnectionMultiplexer.GetEndPoints();
            var targetKeys = new List<string>();
            
            foreach (var endPoint in endPoints)
            {
                var targetEndpoint = redisConnectionMultiplexer.GetServer(endPoint);

                var targetKeysWithinEndPoint = targetEndpoint
                    .Keys(databaseIndex, string.Concat(typeof(T).Name, "*"))
                    .Select(x => (string)x)
                    .ToList();

                // Ensure only unique keys get retrieved. Again, looks retarded, needs a closer look.
                var uniqueKeys = targetKeysWithinEndPoint.Where(x => !targetKeys.Contains(x)).ToList();

                targetKeys.AddRange(uniqueKeys);
            }

            if (targetKeys.Count < 1)
            {
                return new List<T>();
            }

            var keysToBeRetrieved = targetKeys.Select(x => (RedisKey)x).ToArray();

            var keyValuePairsRetrieved = targetDatabase.StringGet(keysToBeRetrieved);

            if (keyValuePairsRetrieved.Length <= 0) return new List<T>();

            var returnPayload = keyValuePairsRetrieved.Select(x => JsonConvert.DeserializeObject<T>(x)).ToList();

            return returnPayload;
        }

        public void InsertKeyValuePair(string key, T itemToBeSaved, bool optimisticLock = false)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (itemToBeSaved == null) throw new ArgumentNullException("itemToBeSaved");

            var serializedObject = JsonConvert.SerializeObject(itemToBeSaved);

            InsertKeyValuePair(key, serializedObject, optimisticLock);
        }

        public bool InsertKeyValuePair(string key, string jsonSerializedItemToBeSaved, bool optimisticLock = false)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (jsonSerializedItemToBeSaved == null) throw new ArgumentNullException("jsonSerializedItemToBeSaved");

            if (optimisticLock)
            {
                // TODO: Write Redlock.NET optimistic locking
                //EnsureAndUpdateTypeSet(RedisConnectionMultiPlexer, DatabaseIndex, key);
            }

            return InsertKeyValuePairNoLock(RedisConnectionMultiPlexer, DatabaseIndex, key, jsonSerializedItemToBeSaved);
        }


        private static bool InsertKeyValuePairNoLock(
            ConnectionMultiplexer connectionMultiplexer, 
            int databaseIndex,
            string key,
            string value)
        {
            if (connectionMultiplexer == null) throw new ArgumentNullException("connectionMultiplexer");
            if (key == null) throw new ArgumentNullException("key");
            if (value == null) throw new ArgumentNullException("value");

            var dataBase = connectionMultiplexer.GetDatabase(databaseIndex);

            /**
             * In Redis Jiggery Pokery, we maintain a set based on object type.
             * This would enable retrieval based on object types and this is 
             * facilated by having a set that stores all the keys.
             * 
             * The transaction will ensure that the key value pair insertion and the 
             * corresponding set entry will go hand in hand.
             * **/
            var transaction = dataBase.CreateTransaction();
            transaction.StringSetAsync(key, value);
            transaction.SetAddAsync(typeof (T).Name, key);
            var transactionSuccessful = transaction.Execute();

            return transactionSuccessful;
        }
    }
}
