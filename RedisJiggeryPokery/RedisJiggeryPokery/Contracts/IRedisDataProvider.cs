using System.Collections.Generic;

namespace RedisJiggeryPokery.Contracts
{
    public interface IRedisDataProvider<T>
    {
        string ConnectionString { get; set; }
        int DatabaseIndex { get; set; }

        IList<T> GetAllKeyValuePair(int dbIndex = 0);

        void InsertKeyValuePair(string key, T itemToBeSaved, bool optimisticLock = false);

        bool InsertKeyValuePair(string key, string jsonSerializedItemToBeSaved, bool optimisticLock = false);
    }
}