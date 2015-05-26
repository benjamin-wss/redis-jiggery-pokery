using System.Collections.Generic;

namespace RedisJiggeryPokery.Contracts
{
    public interface IRedisDataProvider<T>
    {
        string ConnectionString { get; set; }
        int DatabaseIndex { get; set; }

        IList<T> GetAllKeyValuePair(int dbIndex = 0);
    }
}