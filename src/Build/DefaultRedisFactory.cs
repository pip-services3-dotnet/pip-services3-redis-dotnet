using System;
using PipServices.Components.Build;
using PipServices.Commons.Refer;
using PipServices.Redis.Cache;
using PipServices.Redis.Lock;

namespace PipServices.Redis.Build
{
    /// <summary>
    /// Creates Redis components by their descriptors.
    /// </summary>
    /// See <a href="https://rawgit.com/pip-services-dotnet/pip-services-redis-dotnet/master/doc/api/class_pip_services_1_1_redis_1_1_cache_1_1_redis_cache.html">RedisCache</a>, 
    /// <a href="https://rawgit.com/pip-services-dotnet/pip-services-redis-dotnet/master/doc/api/class_pip_services_1_1_redis_1_1_lock_1_1_redis_lock.html">RedisLock</a>
    public class DefaultRedisFactory: Factory
    {
        public static readonly Descriptor Descriptor = new Descriptor("pip-services", "factory", "redis", "default", "1.0");
        public static readonly Descriptor RedisCacheDescriptor = new Descriptor("pip-services", "cache", "redis", "*", "1.0");
        public static readonly Descriptor RedisLockDescriptor = new Descriptor("pip-services", "lock", "redis", "*", "1.0");

        /// <summary>
        /// Create a new instance of the factory.
        /// </summary>
        public DefaultRedisFactory()
        {
            RegisterAsType(DefaultRedisFactory.RedisCacheDescriptor, typeof(RedisCache));
            RegisterAsType(DefaultRedisFactory.RedisLockDescriptor, typeof(RedisLock));
        }
    }
}
