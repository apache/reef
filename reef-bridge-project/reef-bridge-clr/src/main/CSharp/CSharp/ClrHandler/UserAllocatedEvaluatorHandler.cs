using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Reef.Interop;

namespace ClrHandler
{
    public class UserAllocatedEvaluatorHandler : IObserver<AllocatedEvaluator>
    {

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnNext(AllocatedEvaluator value)
        {
            Console.WriteLine("UserAllocatedEvaluatorHandler OnNext 1");
            byte[] context = new byte[2];
            context[0] = 0x10;
            context[1] = 0x20;
            value.Clr2Java.Emanager_submit(context);
            Console.WriteLine("UserAllocatedEvaluatorHandler OnNext 2");
        }
    }
}
