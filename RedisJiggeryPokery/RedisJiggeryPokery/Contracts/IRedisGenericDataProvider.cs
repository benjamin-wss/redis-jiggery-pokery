using System.Collections.Generic;
using StackExchange.Redis;

namespace RedisJiggeryPokery.Contracts
{
    public interface IRedisGenericDataProvider<T>
    {
        /// <summary>
        /// Configuration of the Redis connection.
        /// </summary>
        ConfigurationOptions RedisConfigurationOptions { get; set; }

        /// <summary>
        /// Specifies the default redis database index number of the database
        /// </summary>
        int RedisDatabaseIndex { get; set; }

        /// <summary>
        /// Get all items as values from the Redis key pair store.
        /// </summary>
        /// <param name="dbIndex">Index of the Redis data store. Defaults to 0 as per Redis defaults.</param>
        /// <returns>A collection of typed data stores from Redis.</returns>
        IList<T> GetAllValues(int dbIndex = 0);

        /// <summary>
        /// Gets all items as a key pair value from Redis.
        /// </summary>
        /// <param name="dbIndex">Index of the Redis data store. Defaults to 0 as per Redis defaults.</param>
        /// <returns>A dictionary/hash set of key values.</returns>
        IDictionary<string, T> GetAllKeyValuePairs(int dbIndex = 0);

        /// <summary>
        /// Inserts or updates values that belong to this Key.
        /// </summary>
        /// <param name="key">Key of item that is to be retrieved from Redis.</param>
        /// <param name="itemToBeSaved">Object to be saved.</param>
        /// <param name="dbIndex">Index of the Redis data store. Defaults to 0 as per Redis defaults.</param>
        /// <param name="optimisticLock">Determines should this be a locked update/insert to prevent concurrent access</param>
        /// <returns>Boolean value that states if the insert has been successful.</returns>
        bool InsertOrUpdateKeyValuePair(string key, T itemToBeSaved, int dbIndex = 0, bool optimisticLock = false);

        bool InsertOrUpdateKeyValuePair(string key, string jsonSerializedItemToBeSaved, int dbIndex = 0, bool optimisticLock = false);

        T GetKeyValuePairByKey(string key, int dbIndex = 0);

        IList<T> GetKeyValuePairsByKey(string[] key, int dbIndex = 0);

        bool DeleteKeyValuePair(string key, int dbIndex = 0, bool optimisticLock = false);

        bool DeleteKeyValuePair(string[] key, int dbIndex = 0, bool optimisticLock = false);

        IList<string> GetKeysInSet(int dbIndex = 0);
    }
}