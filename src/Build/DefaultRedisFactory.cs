using System;
using PipServices3.Components.Build;
using PipServices3.Commons.Refer;
using PipServices3.Redis.Cache;
using PipServices3.Redis.Lock;

namespace PipServices3.Redis.Build
{
    /// <summary>
    /// Creates Redis components by their descriptors.
    /// </summary>
    /// See <a href="https://pip-services3-dotnet.github.io/pip-services3-redis-dotnet/class_pip_services_1_1_redis_1_1_cache_1_1_redis_cache.html">RedisCache</a>, 
    /// <a href="https://pip-services3-dotnet.github.io/pip-services3-redis-dotnet/class_pip_services_1_1_redis_1_1_lock_1_1_redis_lock.html">RedisLock</a>
    public class DefaultRedisFactory: Factory
    {
        public static readonly Descriptor Descriptor = new Descriptor("pip-services", "factory", "redis", "default", "1.0");
        public static readonly Descriptor Descriptor3 = new Descriptor("pip-services3", "factory", "redis", "default", "1.0");
        public static readonly Descriptor RedisCacheDescriptor = new Descriptor("pip-services", "cache", "redis", "*", "1.0");
        public static readonly Descriptor RedisCache3Descriptor = new Descriptor("pip-services3", "cache", "redis", "*", "1.0");
        public static readonly Descriptor RedisLockDescriptor = new Descriptor("pip-services", "lock", "redis", "*", "1.0");
        public static readonly Descriptor RedisLock3Descriptor = new Descriptor("pip-services3", "lock", "redis", "*", "1.0");

        /// <summary>
        /// Create a new instance of the factory.
        /// </summary>
        public DefaultRedisFactory()
        {
            RegisterAsType(DefaultRedisFactory.RedisCacheDescriptor, typeof(RedisCache));
            RegisterAsType(DefaultRedisFactory.RedisCache3Descriptor, typeof(RedisCache));
            RegisterAsType(DefaultRedisFactory.RedisLockDescriptor, typeof(RedisLock));
            RegisterAsType(DefaultRedisFactory.RedisLock3Descriptor, typeof(RedisLock));
        }
    }
}
