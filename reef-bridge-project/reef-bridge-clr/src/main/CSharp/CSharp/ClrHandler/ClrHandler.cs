using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Microsoft.Reef.Interop
{
    public class ClrHandler : IObserver<byte[]>
    {
        IInteropReturnInfo _interopReturnInfo;
        ILogger _iLogger;
        public ClrHandler(IInteropReturnInfo interopReturnInfo, ILogger iLogger, String str)
        {
            _interopReturnInfo = interopReturnInfo;
            _iLogger = iLogger;
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
            try
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
            }
            catch (Exception ex)
            {
                _interopReturnInfo.AddExceptionString(ex.Message + ex.StackTrace);
                _interopReturnInfo.SetReturnCode(255);                
            }

        }
    }
}
