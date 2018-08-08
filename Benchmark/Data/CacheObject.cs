using System.Runtime.Serialization;

namespace Benchmark.Data
{
    [DataContract]
    public class DummyCacheObject
    {
        [DataMember]
        public string Id { get; set; }

        [DataMember]
        public string Name { get; set; }

        [DataMember]
        public string Resource { get; set; }
    }
}
