using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;
using Microsoft.DataPipeline.ComputeService.StreamInsightTask;

namespace com.microsoft.reef.activity
{
    public class StreamActivity2 : IActivity
    {
        [Inject]
        public StreamActivity2()
        {
        }

        public byte[] Call(byte[] memento)
        {
            System.Console.WriteLine("Hello, Streaming 2!!");

            SISecondNode();

            return null;
        }

        public static void SISecondNode()
        {
            var a = new SISecondNodeXAM();
            a.Process(2222, 1111);
        }
    }
}
