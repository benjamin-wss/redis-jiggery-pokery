using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RedisJiggeryPokery.Contracts;
using RedLock;
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

        public string RedisConnectionString
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

        public int RedisDatabaseIndex
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
                    ConnectionMultiplexer.Connect(RedisConnectionString);
                }

                return _redisConnectionMultiPlexer;
            }
            set { _redisConnectionMultiPlexer = value; }
        }

        #region GetAllValues
        public IList<T> GetAllValues(int dbIndex = 0)
        {
            var redisDatabaseIndex = dbIndex == 0 ? RedisDatabaseIndex : dbIndex;

            var keysInSet = GetAllKeysForDataTypeByObjectType(typeof (T), RedisConnectionMultiPlexer, redisDatabaseIndex);

            var targetDatabase = RedisConnectionMultiPlexer.GetDatabase(redisDatabaseIndex);

            return keysInSet.Count > 0 ? 
                GetValuesBasedOnSetValues(targetDatabase, keysInSet) : 
                GetAllValuesByWildcard(RedisConnectionMultiPlexer, targetDatabase, RedisDatabaseIndex);
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

        #endregion

        #region InsertKeyValuePair

        public void InsertKeyValuePair(string key, T itemToBeSaved, int dbIndex = 0, bool optimisticLock = false)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (itemToBeSaved == null) throw new ArgumentNullException("itemToBeSaved");

            var serializedObject = JsonConvert.SerializeObject(itemToBeSaved);

            InsertKeyValuePair(key, serializedObject, dbIndex, optimisticLock);
        }

        public bool InsertKeyValuePair(string key, string jsonSerializedItemToBeSaved, int dbIndex = 0, bool optimisticLock = false)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (jsonSerializedItemToBeSaved == null) throw new ArgumentNullException("jsonSerializedItemToBeSaved");

            var redisDatabaseIndex = dbIndex == 0 ? RedisDatabaseIndex : dbIndex;

            if (optimisticLock)
            {
                return InsertKeyValuePairWithOptimisticLockAndNoRetries(
                    RedisConnectionMultiPlexer,
                    redisDatabaseIndex, 
                    key,
                    jsonSerializedItemToBeSaved);
            }

            return InsertKeyValuePairTransaction(RedisConnectionMultiPlexer, redisDatabaseIndex, key, jsonSerializedItemToBeSaved);
        }

        private static bool InsertKeyValuePairWithOptimisticLockAndNoRetries(
            ConnectionMultiplexer connectionMultiplexer, 
            int databaseIndex, 
            string key, 
            string value)
        {
            var endPoint = new[]
            {
                connectionMultiplexer.GetEndPoints().First()
            };

            var expiry = TimeSpan.FromSeconds(30);

            using (var redisLockFactory = new RedisLockFactory(endPoint))
            {
                using (var redisLock = redisLockFactory.Create(key, expiry))
                {
                    if (redisLock.IsAcquired)
                    {
                        return InsertKeyValuePairTransaction(connectionMultiplexer, databaseIndex, key, value);
                    }
                }
            }

            return false;
        }

        private static bool InsertKeyValuePairTransaction(
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

        #endregion

        #region GetAllKeyValuePairs

        public IDictionary<string, T> GetAllKeyValuePairs(int dbIndex = 0)
        {
            var redisDatabaseIndex = dbIndex == 0 ? RedisDatabaseIndex : dbIndex;

            var keysInSet = GetAllKeysForDataTypeByObjectType(typeof(T), RedisConnectionMultiPlexer, redisDatabaseIndex);

            if (keysInSet.Count > 0)
            {
                return ReturnValuesBasedOnKeySet(keysInSet, RedisConnectionMultiPlexer, redisDatabaseIndex);
            }

            return ReturnValuesViaWildcardSearch(typeof(T), RedisConnectionMultiPlexer, redisDatabaseIndex);
        }

        private static IDictionary<string, T> ReturnValuesBasedOnKeySet(
            IList<RedisValue> keysInSet,
            ConnectionMultiplexer connectionMultiplexer, 
            int dbIndex)
        {
            if (keysInSet == null) throw new ArgumentNullException("keysInSet");
            if (connectionMultiplexer == null) throw new ArgumentNullException("connectionMultiplexer");

            var returnValue = new ConcurrentDictionary<string, T>();
            var targetDatabase = connectionMultiplexer.GetDatabase(dbIndex);

            Parallel.ForEach(keysInSet, keyInSet =>
            {
                var key = (string)keyInSet;
                var value = targetDatabase.StringGet((string)keyInSet);
                returnValue.TryAdd(key, JsonConvert.DeserializeObject<T>(value));
            });

            return returnValue;
        }

        private static IDictionary<string, T> ReturnValuesViaWildcardSearch(
            Type targetType,
            ConnectionMultiplexer connectionMultiplexer,
            int dbIndex)
        {
            if (targetType == null) throw new ArgumentNullException("targetType");
            if (connectionMultiplexer == null) throw new ArgumentNullException("connectionMultiplexer");

            var endPoints = connectionMultiplexer.GetEndPoints();
            var keyPrefix = targetType.Namespace;
            var targetKeys = new List<string>();

            foreach (var endPoint in endPoints)
            {
                var targetEndpoint = connectionMultiplexer.GetServer(endPoint);

                var targetKeysWithinEndPoint = targetEndpoint
                    .Keys(dbIndex, string.Concat(keyPrefix, "*"))
                    .Select(x => (string)x)
                    .ToList();

                // Ensure only unique keys get retrieved. Again, looks retarded, needs a closer look.
                var uniqueKeys = targetKeysWithinEndPoint.Where(x => !targetKeys.Contains(x)).ToList();

                targetKeys.AddRange(uniqueKeys);
            }

            var targetDatabase = connectionMultiplexer.GetDatabase(dbIndex);
            var returnPayload = new ConcurrentDictionary<string, T>();

            Parallel.ForEach(targetKeys, targetKey =>
            {
                var returnValue = targetDatabase.StringGet(targetKey);
                var returnValueCasted = JsonConvert.DeserializeObject<T>(returnValue);
                returnPayload.TryAdd(targetKey, returnValueCasted);
            });

            return returnPayload;
        }

        #endregion

        public T GetKeyValuePairByKey(string key, int dbIndex = 0)
        {
            var request = new[]
            {
                key
            };

            return GetKeyValuePairsByKey(request, dbIndex).FirstOrDefault();
        }

        public IList<T> GetKeyValuePairsByKey(string[] key, int dbIndex = 0)
        {
            var redisDatabaseIndex = dbIndex == 0 ? RedisDatabaseIndex : dbIndex;

            var database = RedisConnectionMultiPlexer.GetDatabase(redisDatabaseIndex);

            var redisKeys = key.Select(x => (RedisKey) x).ToArray();
            var retrievedValues = database.StringGet(redisKeys);

            var strongTypedValaues = retrievedValues.Select(x => JsonConvert.DeserializeObject<T>(x)).ToList();

            return strongTypedValaues;
        }

        #region GenericHelpers

        private static IList<RedisValue> GetAllKeysForDataTypeByObjectType(
            Type targetType, 
            ConnectionMultiplexer connectionMultiplexer,
            int databaseIndex)
        {
            var targetDatabase = connectionMultiplexer.GetDatabase(databaseIndex);
            var targetTypeNameString = targetType.Name;
            var keysInSet = targetDatabase.SetMembers(targetTypeNameString).ToList();

            return keysInSet;
        }

        #endregion
    }
}
