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
        public IClr2Java Clr2Java { get; set; }

        public AllocatedEvaluator(IClr2Java clr2Java, byte[] input)
        {
            InstanceId = Encoding.ASCII.GetString(input);
            Clr2Java = clr2Java;
        }

        public void Dispose()
        {
        }
    }
}
