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
            value.Clr2Java.AllocatedEvaluatorSubmitContextAndTask(value.ContextConfigStr, value.TaskConfigStr);
            Console.WriteLine("UserAllocatedEvaluatorHandler OnNext 2");
        }
    }
}
