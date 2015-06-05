using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Newtonsoft.Json;
using RedisJiggeryPokery.Contracts;
using RedisJiggeryPokery.Exceptions;
using RedLock;
using StackExchange.Redis;

namespace RedisJiggeryPokery
{
    public class RedisGenericDataProvider<T> : IRedisGenericDataProvider<T>
    {
        private ConfigurationOptions _redisConfigurationOptions;
        private int _databaseIndex;
        private ConnectionMultiplexer _redisConnectionMultiPlexer;

        /// <summary>
        /// Constructor for the data provider.
        /// </summary>
        /// <param name="redisConfigurationOptions">
        /// Configuration for the redis connection. 
        /// </param>
        /// <param name="targetDatabaseIndex">
        /// Target Redis Database Index.
        /// Defaults to 0 as per StackExchange.Redis API defaults.
        /// </param>
        public RedisGenericDataProvider(
            ConfigurationOptions redisConfigurationOptions,
            int targetDatabaseIndex = 0)
        {
            _databaseIndex = targetDatabaseIndex;
            _redisConfigurationOptions = redisConfigurationOptions;
            _redisConnectionMultiPlexer = ConnectionMultiplexer.Connect(redisConfigurationOptions);
        }

        public ConfigurationOptions RedisConfigurationOptions
        {
            get { return _redisConfigurationOptions; }
            set
            {
                _redisConfigurationOptions = value;

                // forces a reset of the multiplexer of the configuration has been changed
                _redisConnectionMultiPlexer = null;
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
                    _redisConnectionMultiPlexer = ConnectionMultiplexer.Connect(RedisConfigurationOptions);
                }

                return _redisConnectionMultiPlexer;
            }
        }

        #region GetAllValues

        /// <summary>
        /// This will get all values based on the object specified when instantiating the dataprovider.
        /// </summary>
        /// <param name="dbIndex">
        /// Redis database index. This defaults to 0 as per StackExchange.Redis defaults.
        /// </param>
        /// <returns>
        /// Returns all items that belonging to this object type in the Redis datastore as a list.
        /// </returns>
        public IList<T> GetAllValues(int dbIndex = 0)
        {
            var redisDatabaseIndex = dbIndex == 0 ? RedisDatabaseIndex : dbIndex;

            var keysInSet = GetAllKeysForDataTypeByObjectType(typeof(T), RedisConnectionMultiPlexer, redisDatabaseIndex);

            var targetDatabase = RedisConnectionMultiPlexer.GetDatabase(redisDatabaseIndex);

            return keysInSet.Count > 0 ?
                GetValuesBasedOnSetValues(targetDatabase, keysInSet) :
                GetAllValuesByWildcard(RedisConnectionMultiPlexer, targetDatabase, RedisDatabaseIndex);
        }

        /// <summary>
        /// A helper function to retrieve items based on the object type. 
        /// This function will retrieve values based on te object type in the object type Redis Set.
        /// 
        /// This will only be triggered if the set exists.
        /// </summary>
        /// <param name="targetDatabase">The database specified in the cller function.</param>
        /// <param name="keysRetrievedFromSet">Keys retrieved from a set based on object name.</param>
        /// <returns></returns>
        private static IList<T> GetValuesBasedOnSetValues(
            [NotNull] IDatabase targetDatabase,
            [NotNull] IList<RedisValue> keysRetrievedFromSet)
        {
            if (targetDatabase == null) throw new ArgumentNullException("targetDatabase");
            if (keysRetrievedFromSet == null) throw new ArgumentNullException("keysRetrievedFromSet");

            // TODO : Need to examine if the parallel stuff actually has benefits.
            var redisKeys = keysRetrievedFromSet
                .Select(x => (RedisKey) ((string) x))
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
                    .Keys(databaseIndex, string.Concat(typeof (T).Name, "*"))
                    .Select(x => (string) x)
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

            var keysToBeSavedInSeet = targetKeys.Select(x => (RedisValue)x).ToArray();

            targetDatabase.SetAdd(typeof(T).Name, keysToBeSavedInSeet);

            var returnPayload = keyValuePairsRetrieved.Select(x => JsonConvert.DeserializeObject<T>(x)).ToList();

            return returnPayload;
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

        #region InsertOrUpdateKeyValuePair

        public bool InsertOrUpdateKeyValuePair(string key, T itemToBeSaved, int dbIndex = 0, bool optimisticLock = false)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (itemToBeSaved == null) throw new ArgumentNullException("itemToBeSaved");

            var serializedObject = JsonConvert.SerializeObject(itemToBeSaved);

            return InsertOrUpdateKeyValuePair(key, serializedObject, dbIndex, optimisticLock);
        }

        public bool InsertOrUpdateKeyValuePair(string key, string jsonSerializedItemToBeSaved, int dbIndex = 0,
            bool optimisticLock = false)
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

                    throw RedisOptimisticLockException.GenerateBasicException(
                        key, 
                        value, 
                        "Unable to save item because it is locked. Please try again.");
                }
            }
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
            transaction.SetAddAsync(typeof(T).Name, key);
            var transactionSuccessful = transaction.Execute();

            return transactionSuccessful;
        }

        #endregion

        #region GetKeyValuePairByKey

        public T GetKeyValuePairByKey(string key, int dbIndex = 0)
        {
            var request = new[]
            {
                key
            };

            return GetKeyValuePairsByKey(request, dbIndex).FirstOrDefault();
        }

        #endregion

        #region GetKeyValuePairsByKey

        public IList<T> GetKeyValuePairsByKey(string[] key, int dbIndex = 0)
        {
            var redisDatabaseIndex = dbIndex == 0 ? RedisDatabaseIndex : dbIndex;

            var database = RedisConnectionMultiPlexer.GetDatabase(redisDatabaseIndex);

            var redisKeys = key.Select(x => (RedisKey)x).ToArray();
            var retrievedValues = database.StringGet(redisKeys);

            var castedValues = retrievedValues.Where(x => x.IsNullOrEmpty == false).Select(x => JsonConvert.DeserializeObject<T>(x)).ToList();

            return castedValues;
        }

        #endregion

        #region DeleteKeyValuePair

        public bool DeleteKeyValuePair(string key, int dbIndex = 0, bool optimisticLock = false)
        {
            if (key == null) throw new ArgumentNullException("key");

            var redisKey = new[]
            {
                (RedisKey) key
            };

            var redisDatabaseIndex = dbIndex == 0 ? RedisDatabaseIndex : dbIndex;

            return DeleteKeyValuePair(
                redisKey,
                RedisConnectionMultiPlexer,
                redisDatabaseIndex,
                optimisticLock);
        }

        public bool DeleteKeyValuePair(string[] key, int dbIndex = 0, bool optimisticLock = false)
        {
            if (key == null) throw new ArgumentNullException("key");

            var redisDatabaseIndex = dbIndex == 0 ? RedisDatabaseIndex : dbIndex;

            return DeleteKeyValuePair(
                key.Select(x => (RedisKey)x).ToArray(),
                RedisConnectionMultiPlexer,
                redisDatabaseIndex, optimisticLock);
        }

        private static bool DeleteKeyValuePair(
            RedisKey[] keysToBeDeleted,
            ConnectionMultiplexer connectionMultiplexer,
            int dbIndex,
            bool optimisticLock)
        {
            if (keysToBeDeleted == null) throw new ArgumentNullException("keysToBeDeleted");
            if (connectionMultiplexer == null) throw new ArgumentNullException("connectionMultiplexer");

            if (optimisticLock)
            {
                return DeleteKeyValuePair_OptimisticLock_NoRetries(keysToBeDeleted, connectionMultiplexer, dbIndex);
            }

            return DeleteKeyValuePair_NoLock(keysToBeDeleted, connectionMultiplexer, dbIndex);
        }

        private static bool DeleteKeyValuePair_OptimisticLock_NoRetries(
            RedisKey[] keysToBeDeleted,
            ConnectionMultiplexer connectionMultiplexer,
            int dbIndex)
        {
            if (keysToBeDeleted == null) throw new ArgumentNullException("keysToBeDeleted");
            if (connectionMultiplexer == null) throw new ArgumentNullException("connectionMultiplexer");

            var endPoint = new[]
            {
                connectionMultiplexer.GetEndPoints().First()
            };

            var expiry = TimeSpan.FromSeconds(30);

            using (var redisLockFactory = new RedisLockFactory(endPoint))
            {
                var exceptions = new Collection<RedisOptimisticLockException>();

                foreach (var redisKey in keysToBeDeleted)
                {
                    using (var redisLock = redisLockFactory.Create(redisKey, expiry))
                    {
                        if (redisLock.IsAcquired)
                        {
                            var targetKey = new[]
                            {
                                redisKey
                            };

                            DeleteKeyValuePair_NoLock(targetKey, connectionMultiplexer, dbIndex);
                        }
                        else
                        {
                            var exception = RedisOptimisticLockException.GenerateBasicException(redisKey, "N/A", "Unable to delete item because it is locked. Please try again.");

                            exceptions.Add(exception);
                        }
                    }
                }

                if (exceptions.Count > 0)
                {
                    var stringBuilder = new StringBuilder();

                    const string messageFormat = "{0}. {1}";

                    for (var i = 0; i < exceptions.Count; i++)
                    {
                        var exception = exceptions[i];

                        var formattedMessage = string.Format(messageFormat, i, exception.Message);

                        stringBuilder.AppendLine(formattedMessage);
                    }

                    throw RedisOptimisticLockException.GenerateBasicException(stringBuilder.ToString());
                }
            }

            return false;
        }

        private static bool DeleteKeyValuePair_NoLock(
            RedisKey[] keysToBeDeleted,
            ConnectionMultiplexer connectionMultiplexer,
            int dbIndex)
        {
            if (keysToBeDeleted == null) throw new ArgumentNullException("keysToBeDeleted");
            if (connectionMultiplexer == null) throw new ArgumentNullException("connectionMultiplexer");

            var targetDatabase = connectionMultiplexer.GetDatabase(dbIndex);

            var returnValue = targetDatabase.KeyDelete(keysToBeDeleted);

            if (returnValue != 0)
            {
                var setKeysToBeRemoved = keysToBeDeleted.Select(x => (RedisValue)((string)x)).ToArray();

                targetDatabase.SetRemove(typeof(T).Name, setKeysToBeRemoved);

                return true;
            }

            return false;
        }

        #endregion

        #region GetKeysInSet

        public IList<string> GetKeysInSet(int dbIndex = 0)
        {
            var targetDatabase = RedisConnectionMultiPlexer.GetDatabase(dbIndex);

            var keysInSet = targetDatabase.SetMembers(typeof(T).Name);

            var returnPayload = keysInSet.Select(x => (string)x).ToList();

            return returnPayload;
        }

        #endregion

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
