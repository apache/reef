using System;
using System.Runtime.Serialization;
using System.Text;

namespace Microsoft.Reef.Interop
{
    [DataContract]
    public class AllocatedEvaluator : IDisposable
    {
        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public IAllocatedEvaluaotrClr2Java Clr2Java { get; set; }

        public AllocatedEvaluator(IAllocatedEvaluaotrClr2Java clr2Java)
        {
            InstanceId = Guid.NewGuid().ToString("N");
            Clr2Java = clr2Java;
        }

        public void Dispose()
        {
        }
    }
}
