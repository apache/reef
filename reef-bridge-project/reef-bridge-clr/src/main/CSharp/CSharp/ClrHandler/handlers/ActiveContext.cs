using System;
using System.Runtime.Serialization;
using System.Text;

namespace Microsoft.Reef.Interop
{
    [DataContract]
    public class ActiveContext : IDisposable
    {
        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public IActiveContextClr2Java Clr2Java { get; set; }

        public ActiveContext(IActiveContextClr2Java clr2Java)
        {
            InstanceId = Guid.NewGuid().ToString("N");
            Clr2Java = clr2Java;
        }

        public void Dispose()
        {
        }
    }
}
