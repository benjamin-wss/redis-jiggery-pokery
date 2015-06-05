using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using RedisJiggeryPokery.Exceptions;
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

        private static ConfigurationOptions RedisConfigurationOptions
        {
            get
            {
                return _redisConfigurationOptions ?? 
                (
                    _redisConfigurationOptions = new ConfigurationOptions()
                    {
                        EndPoints = {"localhost"},
                        AllowAdmin = true
                    }
                );
            }
        }

        [TestInitialize]
        public void SetupTestEnvironment()
        {
            ClearRedisDbOfValues(RedisConfigurationOptions);
        }

        [TestMethod]
        public void GetAllAsValue_RedisPopulated_Success()
        {
            GenerateDataSet(RedisConfigurationOptions);

            var redisDataProvider = new RedisGenericDataProvider<SampleTestObject>(RedisConfigurationOptions);
            var returnedPayload = redisDataProvider.GetAllAsValues();

            Assert.IsTrue(returnedPayload.Count == 100, "Object did not retrieve all of prepopulated values. Network issues may be at play");
            Assert.IsTrue(returnedPayload.OfType<SampleTestObject>().ToList().Count == 100, "Object retireved is not of correct type");
        }

        [TestMethod]
        public void SaveKeyValuePair_NoLock_Success()
        {
            var redisDataProvider = new RedisGenericDataProvider<SampleTestObject>(RedisConfigurationOptions);

            var currentSessionGuid = Guid.NewGuid();

            var sampleSave = new SampleTestObject()
            {
                Description = "Hahahahahaha",
                Id = currentSessionGuid
            };

            var saveSuccessful = redisDataProvider.Set(currentSessionGuid.ToString(), sampleSave);

            Assert.IsTrue(saveSuccessful, "Unable to save");

            var returnedValue = redisDataProvider.Get(currentSessionGuid.ToString());

            Assert.IsTrue(sampleSave.Id == returnedValue.Id, "Id field returned does not match");
            Assert.IsTrue(sampleSave.Description == returnedValue.Description, "Description field returned does not match");
        }

        [TestMethod]
        public void SaveKeyValuePair_LockWithNoConcurrentEntries_Success()
        {
            var redisDataProvider = new RedisGenericDataProvider<SampleTestObject>(RedisConfigurationOptions);

            var currentSessionGuid = Guid.NewGuid();

            var sampleSave = new SampleTestObject()
            {
                Description = "Hahahahahaha",
                Id = currentSessionGuid
            };

            var saveSuccessful = redisDataProvider.Set(currentSessionGuid.ToString(), sampleSave, 0, true);

            Assert.IsTrue(saveSuccessful, "Unable to save");

            var returnedValue = redisDataProvider.Get(currentSessionGuid.ToString());

            Assert.IsTrue(sampleSave.Id == returnedValue.Id, "Id field returned does not match");
            Assert.IsTrue(sampleSave.Description == returnedValue.Description, "Description field returned does not match");
        }

        [TestMethod]
        public void SaveKeyValuePair_LockWithConcurrentEntries_Success()
        {
            var redisDataProvider = new RedisGenericDataProvider<SampleTestObject>(RedisConfigurationOptions);

            var currentSessionGuid = Guid.NewGuid();

            var lockDetected = false;

            var sampleSave = new SampleTestObject()
            {
                Description = "Hahahahahaha",
                Id = currentSessionGuid
            };

            // Simulating concurrent access.
            Parallel.Invoke(() =>
            {
                // done by hand to ensure very granular control of the lock.
                var connectionMultiplexer = ConnectionMultiplexer.Connect(RedisConfigurationOptions);

                using (var redlockFactory = new RedisLockFactory(connectionMultiplexer.GetEndPoints()))
                {
                    var sessionId = string.Concat(sampleSave.GetType().Name, "_", currentSessionGuid.ToString());

                    using (var optimisticLock = redlockFactory.Create(sessionId, TimeSpan.FromSeconds(6000)))
                    {
                        if (optimisticLock.IsAcquired)
                        {
                            redisDataProvider.Set(currentSessionGuid.ToString(), sampleSave);
                            Thread.Sleep(6000);
                        }
                    }
                }
            }, () =>
            {
                // Ensures the first job is always done first.
                Thread.Sleep(3000);

                var sampleSave2 = new SampleTestObject()
                {
                    Description = "tech21",
                    Id = currentSessionGuid
                };

                try
                {
                    redisDataProvider.Set(sampleSave2.Id.ToString(), sampleSave2, 0, true);
                }
                catch (RedisOptimisticLockException redisOptimisticLockException)
                {
                    lockDetected = true;
                }
            });

            Assert.IsTrue(lockDetected, "Lock was not detected. Optimistic lock failed to engage.");

            var returnedValue = redisDataProvider.Get(currentSessionGuid.ToString());

            Assert.IsTrue(sampleSave.Id == returnedValue.Id, "Id field returned does not match");
            Assert.IsTrue(sampleSave.Description == returnedValue.Description, "Description field returned does not match");
        }

        [TestCleanup]
        public void CleanupTestData()
        {
            ClearRedisDbOfValues(RedisConfigurationOptions);
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

        private static void GenerateDataSet(
            [NotNull] ConfigurationOptions redisConfigurationOptions,
            int numberOfItems = 100,
            int dbIndex = 0)
        {
            if (redisConfigurationOptions == null) throw new ArgumentNullException("redisConfigurationOptions");

            var connectionMultiplexer = ConnectionMultiplexer.Connect(redisConfigurationOptions);
            var targetDatabase = connectionMultiplexer.GetDatabase(dbIndex);

            Parallel.For(0, numberOfItems, i =>
            {
                var sampleTestObject = new SampleTestObject()
                {
                    Description = string.Concat("Item number ", i.ToString()),
                    Id = Guid.NewGuid()
                };

                var key = string.Concat(sampleTestObject.GetType().Name, "_", sampleTestObject.Id.ToString());

                targetDatabase.StringSet(key, JsonConvert.SerializeObject(sampleTestObject));
            });
        }

        #endregion
    }
}
