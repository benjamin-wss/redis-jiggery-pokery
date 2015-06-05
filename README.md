# Redis Jiggery Pokery

## Purpose

Redis Jiggery Pokery is meant to provide a familliar repository pattern esque mechanism to interact with Redis. This would abstract the jiggery pokery needed to work with a key-value store like Redis.
  
You can store your data objects like tables in Active Record where a table is represented like an object. However as Redis is a NoSQL database, this does mean that unlike NHibernate, Dapper, Entity Framework, <insert favourite ORM here>, there are no joins, views or any of that here. Those sort of features would also not be replicated in any way or form as it is defeats the purpose of a key-value store.

## Prerequisites

1. A running Redis instance. If you want one to test, you may download it from (here)[https://github.com/MSOpenTech/redis/releases].
2. Some C# mojo.

## Sample

      public class User
      {
        public Guid UserId 
        { 
          get; 
          set; 
         }
       }
       
       public class UserDataStore()
       {
          private readonly ConfigurationOptions _configurationOptions;
          private readonly RedisGenericDataProvider<User> _redisGenericDataProvider;
          
          public UserDataStore()
          {
            _configurationOptions = new ConfigurationOptions()
            {
              // Url(s) or Redis datastores. In this case we have only one.
              EndPoints = { "localhost" }
            };

            _redisGenericDataProvider = new RedisGenericDataProvider<User>(_configurationOptions);
          }

          public IList<User> GetAll()
          {
            var allItems = _redisGenericDataProvider.GetAllValues();
            return allItems;
          }
       }
