using System;
using PipServices.Components.Lock;
using Xunit;

namespace PipServices.Redis.Lock
{
    public class LockFixture
    {
        public static string LOCK1 = "lock_1";
        public static string LOCK2 = "lock_2";
        public static string LOCK3 = "lock_3";

        private ILock _lock;

        public LockFixture(ILock alock)
        {
            _lock = alock;
        }

        public void TestTryAcquireLock()
        {
            // Try to acquire lock for the first time
            var result = _lock.TryAcquireLock(null, LOCK1, 3000);
            Assert.True(result);

            // Try to acquire lock for the second time
            result = _lock.TryAcquireLock(null, LOCK1, 3000);
            Assert.False(result);

            // Release the lock
            _lock.ReleaseLock(null, LOCK1);

            // Try to acquire lock for the third time
            result = _lock.TryAcquireLock(null, LOCK1, 3000);
            Assert.True(result);

            _lock.ReleaseLock(null, LOCK1);
        }

        public void TestAcquireLock()
        {
            // Acquire lock for the first time
            _lock.AcquireLock(null, LOCK2, 3000, 1000);

            // Acquire lock for the second time
            var result = false;
            try
            {
                _lock.AcquireLock(null, LOCK2, 3000, 1000);
            }
            catch (Exception)
            {
                result = true;
            }
            Assert.True(result);

            // Release the lock
            _lock.ReleaseLock(null, LOCK2);

            // Acquire lock for the third time
            _lock.AcquireLock(null, LOCK2, 3000, 1000);
            _lock.ReleaseLock(null, LOCK2);
        }

        public void TestReleaseLock()
        {
            // Acquire lock for the first time
            var result = _lock.TryAcquireLock(null, LOCK3, 3000);
            Assert.True(result);

            // Release the lock for the first time
            _lock.ReleaseLock(null, LOCK3);

            // Release the lock for the second time
            _lock.ReleaseLock(null, LOCK3);
        }

    }
}
