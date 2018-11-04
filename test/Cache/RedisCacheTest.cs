using System;
using PipServices3.Commons.Config;
using PipServices3.Commons.Convert;
using Xunit;

namespace PipServices3.Redis.Cache
{
    [Collection("Sequential")]
    public class RedisCacheTest: IDisposable
    {
        private readonly bool _enabled;
        private readonly RedisCache _cache;
        private readonly CacheFixture _fixture;

        public RedisCacheTest()
        {
            var REDIS_ENABLED = Environment.GetEnvironmentVariable("REDIS_ENABLED") ?? "true";
            var REDIS_SERVICE_HOST = Environment.GetEnvironmentVariable("REDIS_SERVICE_HOST") ?? "localhost";
            var REDIS_SERVICE_PORT = Environment.GetEnvironmentVariable("REDIS_SERVICE_PORT") ?? "6379";

            _enabled = BooleanConverter.ToBoolean(REDIS_ENABLED);
            if (_enabled)
            {
                _cache = new RedisCache();
                _cache.Configure(ConfigParams.FromTuples(
                    "connection.host", REDIS_SERVICE_HOST,
                    "connection.port", REDIS_SERVICE_PORT
                ));

                _fixture = new CacheFixture(_cache);

                _cache.OpenAsync(null).Wait();
            }
        }

        public void Dispose()
        {
            if (_cache != null)
            {
                _cache.CloseAsync(null).Wait();
            }
        }

        [Fact]
        public void TestStoreAsync()
        {
            _fixture.TestStoreAsync();
        }

        [Fact]
        public void TestStoreAndRetrieveAsync()
        {
            _fixture.TestStoreAndRetrieveAsync();
        }

        [Fact]
        public void TestStoreAndRetrieveExpiredAsync()
        {
            _fixture.TestStoreAndRetrieveExpiredAsync();
        }

        [Fact]
        public void TestStoreAndRetrieveRemovedAsync()
        {
            _fixture.TestStoreAndRetrieveRemovedAsync();
        }
    }
}
