using System.Collections.Generic;
using StackExchange.Redis;

namespace RedisJiggeryPokery.Contracts
{
    public interface IRedisGenericDataProvider<T>
    {
        ConfigurationOptions RedisConfigurationOptions { get; set; }

        /// <summary>
        /// Specifies the default redis database index number of the database
        /// </summary>
        int RedisDatabaseIndex { get; set; }

        IList<T> GetAllValues(int dbIndex = 0);

        IDictionary<string, T> GetAllKeyValuePairs(int dbIndex = 0);

        bool InsertOrUpdateKeyValuePair(string key, T itemToBeSaved, int dbIndex = 0, bool optimisticLock = false);

        bool InsertOrUpdateKeyValuePair(string key, string jsonSerializedItemToBeSaved, int dbIndex = 0, bool optimisticLock = false);

        T GetKeyValuePairByKey(string key, int dbIndex = 0);

        IList<T> GetKeyValuePairsByKey(string[] key, int dbIndex = 0);

        bool DeleteKeyValuePair(string key, int dbIndex = 0, bool optimisticLock = false);

        bool DeleteKeyValuePair(string[] key, int dbIndex = 0, bool optimisticLock = false);

        IList<string> GetKeysInSet(int dbIndex = 0);
    }
}