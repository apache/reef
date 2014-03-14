using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Microsoft.Reef.Interop
{
    public class ClrHandler : IObserver<byte[]>
    {
        public ClrHandler(String str)
        {
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnNext(byte[] value)
        {
            if (value != null)
            {
            // use avro to unpack from byte[] to object
            // foreach bound user handler 
            // call Onnext with the AllocatedEvaluator.
                for (int i = 0; i < value.Length; i++ )
                {
                    Console.WriteLine("[" + i  + "] "  + value[i]);
                }
            }
            Console.WriteLine("OnNext was called Yingda!!!!" + value.Length);
        }
    }
}
