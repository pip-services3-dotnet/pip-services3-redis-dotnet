using System;
using System.Threading.Tasks;
using PipServices3.Commons.Config;
using PipServices3.Commons.Convert;
using Xunit;

namespace PipServices3.Redis.Lock
{
    [Collection("Sequential")]
    public class RedisLockTest : IDisposable
    {
        private readonly bool _enabled;
        private readonly RedisLock _lock;
        private readonly LockFixture _fixture;

        public RedisLockTest()
        {
            var REDIS_ENABLED = Environment.GetEnvironmentVariable("REDIS_ENABLED") ?? "true";
            var REDIS_SERVICE_HOST = Environment.GetEnvironmentVariable("REDIS_SERVICE_HOST") ?? "localhost";
            var REDIS_SERVICE_PORT = Environment.GetEnvironmentVariable("REDIS_SERVICE_PORT") ?? "6379";

            _enabled = BooleanConverter.ToBoolean(REDIS_ENABLED);
            if (_enabled)
            {
                _lock = new RedisLock();
                _lock.Configure(ConfigParams.FromTuples(
                    "connection.host", REDIS_SERVICE_HOST,
                    "connection.port", REDIS_SERVICE_PORT
                ));

                _fixture = new LockFixture(_lock);

                _lock.OpenAsync(null).Wait();
            }
        }

        public void Dispose()
        {
            if (_lock != null)
            {
                _lock.CloseAsync(null).Wait();
            }
        }

        [Fact]
        public void TestAcquireLock()
        {
            _fixture.TestAcquireLock();
        }

        [Fact]
        public void TestTryAcquireLock()
        {
            _fixture.TestTryAcquireLock();
        }

        [Fact]
        public void TestReleaseLock()
        {
            _fixture.TestReleaseLock();
        }
    }
}
