using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

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
        [TestMethod]
        public void SaveKeyValuePair_NoLock_Success()
        {
            var redisDataProvider = new RedisDataProvider<SampleTestObject>("localhost");

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
            var redisDataProvider = new RedisDataProvider<SampleTestObject>("localhost");

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
    }
}
