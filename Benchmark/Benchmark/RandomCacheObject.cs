using System;
using System.Collections.Generic;
using Benchmark.Data;
using PipServices.Commons.Random;

namespace Benchmark.Benchmark
{
    public class RandomDummyCacheObject
    {
        private static string NextId()
        {
            return Guid.NewGuid().ToString("N").Replace("-", "");
        }
        
        public static DummyCacheObject GenerateCacheObject()
        {
            return new DummyCacheObject
            {
                Id = NextId(),
                Name = "CacheObject_" + RandomText.Word(),
                Resource = RandomText.Stuff() + RandomText.Word()
            };
        }
    }
}
