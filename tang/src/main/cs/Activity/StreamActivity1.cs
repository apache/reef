using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;
using Microsoft.DataPipeline.ComputeService.StreamInsightTask;

namespace com.microsoft.reef.activity
{
    public class StreamActivity1 : IActivity
    {
        public string ipAddress;

        [NamedParameter("Ip Address", "IP", "10.121.32.158")]
        class IpAddress : Name<string> { }

        [Inject]
        public StreamActivity1([Parameter(Value = typeof(IpAddress))] string ipAddress)
        {
            this.ipAddress = ipAddress;
        }

        [Inject]
        public StreamActivity1()
        {
        }

        public byte[] Call(byte[] memento)
        {
            System.Console.WriteLine("Hello, Streaming 1!!, Ip: " + ipAddress);

            Thread.Sleep(10000);

            SIFirstNode();

            return null;
        }

        public static void SIFirstNode()
        {
            var a = new SIFirstNodeXAM();
            a.Process(1111, 2222);
        }
    }
}
