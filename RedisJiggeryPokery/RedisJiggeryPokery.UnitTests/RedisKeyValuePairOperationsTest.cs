using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RedLock;
using StackExchange.Redis;

namespace RedisJiggeryPokery.IntegrationTests
{
    public class SampleTestObject
    {
        public Guid Id { get; set; }
        public string Description { get; set; }
    }

    [TestClass]
    public class RedisKeyValuePairOperationsTest
    {
        private static ConfigurationOptions _redisConfigurationOptions;

        [TestInitialize]
        public void SetupTestEnvironment()
        {
            _redisConfigurationOptions = new ConfigurationOptions()
            {
                EndPoints = {"localhost"},
                AllowAdmin = true
            };

            ClearRedisDbOfValues(_redisConfigurationOptions);
        }

        [TestMethod]
        public void SaveKeyValuePair_NoLock_Success()
        {
            var redisDataProvider = new RedisGenericDataProvider<SampleTestObject>(_redisConfigurationOptions);

            var currentSessionGuid = Guid.NewGuid();

            var sampleSave = new SampleTestObject()
            {
                Description = "Hahahahahaha",
                Id = currentSessionGuid
            };

            var saveSuccessful = redisDataProvider.InsertOrUpdateKeyValuePair(currentSessionGuid.ToString(), sampleSave);

            Assert.IsTrue(saveSuccessful, "Unable to save");

            var returnedValue = redisDataProvider.GetKeyValuePairByKey(currentSessionGuid.ToString());

            Assert.IsTrue(sampleSave.Id == returnedValue.Id, "Id field returned does not match");
            Assert.IsTrue(sampleSave.Description == returnedValue.Description, "Description field returned does not match");
        }

        [TestMethod]
        public void SaveKeyValuePair_LockWithNoConcurrentEntries_Success()
        {
            var redisDataProvider = new RedisGenericDataProvider<SampleTestObject>(_redisConfigurationOptions);

            var currentSessionGuid = Guid.NewGuid();

            var sampleSave = new SampleTestObject()
            {
                Description = "Hahahahahaha",
                Id = currentSessionGuid
            };

            var saveSuccessful = redisDataProvider.InsertOrUpdateKeyValuePair(currentSessionGuid.ToString(), sampleSave, 0, true);

            Assert.IsTrue(saveSuccessful, "Unable to save");

            var returnedValue = redisDataProvider.GetKeyValuePairByKey(currentSessionGuid.ToString());

            Assert.IsTrue(sampleSave.Id == returnedValue.Id, "Id field returned does not match");
            Assert.IsTrue(sampleSave.Description == returnedValue.Description, "Description field returned does not match");
        }

        [TestMethod]
        public void SaveKeyValuePair_LockWithConcurrentEntries_Success()
        {
            var redisDataProvider = new RedisGenericDataProvider<SampleTestObject>(_redisConfigurationOptions);

            var currentSessionGuid = Guid.NewGuid();

            var sampleSave = new SampleTestObject()
            {
                Description = "Hahahahahaha",
                Id = currentSessionGuid
            };

            Parallel.Invoke(() =>
            {
                var connectionMultiplexer = ConnectionMultiplexer.Connect(_redisConfigurationOptions);

                using (var redlockFactory = new RedisLockFactory(connectionMultiplexer.GetEndPoints().First()))
                {
                    using (var optimisticLock = redlockFactory.Create(currentSessionGuid.ToString(), TimeSpan.FromSeconds(5000)))
                    {
                        if (optimisticLock.IsAcquired)
                        {
                            redisDataProvider.InsertOrUpdateKeyValuePair(currentSessionGuid.ToString(), sampleSave);
                            Thread.Sleep(5000);
                        }
                    }
                }
            }, () =>
            {
                var connectionMultiplexer = ConnectionMultiplexer.Connect(_redisConfigurationOptions);

                var sampleSave2 = new SampleTestObject()
                {
                    Description = "tech21",
                    Id = currentSessionGuid
                };

                using (var redlockFactory = new RedisLockFactory(connectionMultiplexer.GetEndPoints().First()))
                {
                    using (var optimisticLock = redlockFactory.Create(currentSessionGuid.ToString(), TimeSpan.FromSeconds(30)))
                    {
                        if (optimisticLock.IsAcquired)
                        {
                            redisDataProvider.InsertOrUpdateKeyValuePair(currentSessionGuid.ToString(), sampleSave2);
                        }
                        else
                        {
                            var x = 0;
                        }
                    }
                }
            });

            //var saveSuccessful = redisDataProvider.InsertOrUpdateKeyValuePair(currentSessionGuid.ToString(), sampleSave, 0, true);

            //Assert.IsTrue(saveSuccessful, "Unable to save");

            var returnedValue = redisDataProvider.GetKeyValuePairByKey(currentSessionGuid.ToString());

            Assert.IsTrue(sampleSave.Id == returnedValue.Id, "Id field returned does not match");
            Assert.IsTrue(sampleSave.Description == returnedValue.Description, "Description field returned does not match");
        }

        #region Generic Helpers

        private static void ClearRedisDbOfValues([NotNull] ConfigurationOptions redisConfigurationOptions)
        {
            if (redisConfigurationOptions == null) throw new ArgumentNullException("redisConfigurationOptions");

            var connectionMultiplexer = ConnectionMultiplexer.Connect(redisConfigurationOptions);

            var endPoints = connectionMultiplexer.GetEndPoints();

            Parallel.ForEach(endPoints, endPoint =>
            {
                var targetServer = connectionMultiplexer.GetServer(endPoint);

                targetServer.FlushAllDatabases();
            });
        }

        #endregion
    }
}
