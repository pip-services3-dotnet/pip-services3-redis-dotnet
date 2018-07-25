using System.Threading.Tasks;
using PipServices.Components.Cache;
using Xunit;

namespace PipServices.Redis.Cache
{
    public class CacheFixture
    {
        private const string Key1 = "key1";
        private const string Key2 = "key2";
        private const string Key3 = "key3";

        private const string Value1 = "value1";
        private const string Value2 = "value2";
        private const string Value3 = "value3";

        private ICache _cache;

        public CacheFixture(ICache cache)
        {
            _cache = cache;
        }

        public async void TestStoreAsync()
        {
            var result = await _cache.StoreAsync(null, Key1, Value1, 5000);

            Assert.NotNull(result);
        }

        public async void TestStoreAndRetrieveAsync()
        {
            await _cache.StoreAsync(null, Key1, Value1, 5000);
            await _cache.StoreAsync(null, Key2, Value2, 5000);

            await Task.Delay(500);

            var val1 = _cache.RetrieveAsync<string>(null, Key1).Result;
            var val2 = _cache.RetrieveAsync<string>(null, Key2).Result;

            Assert.NotNull(val1);
            Assert.NotNull(val2);

            Assert.Equal(Value1, val1);
            Assert.Equal(Value2, val2);
        }

        public async void TestStoreAndRetrieveExpiredAsync()
        {
            await _cache.StoreAsync(null, Key1, Value1, 500);

            await Task.Delay(2000);

            var val1 = _cache.RetrieveAsync<string>(null, Key1).Result;

            Assert.Null(val1);
        }

        public async void TestStoreAndRetrieveRemovedAsync()
        {
            await _cache.StoreAsync(null, Key1, Value1, 5000);

            await Task.Delay(500);

            await _cache.RemoveAsync(null, Key1);

            var val1 = _cache.RetrieveAsync<string>(null, Key1).Result;

            Assert.Null(val1);
        }
    }
}
