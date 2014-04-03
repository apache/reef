using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ClrHandler;

namespace Microsoft.Reef.Interop
{
    public class ClrSystemOnStartHandler : IObservable<DateTime> , IObserver <DateTime>
    {
        internal  static ClrSystemAllocatedEvaluatorHandler _allocatedEvaluatorHandler;
        public static ulong[] OnStart(DateTime startTime)
        {

            List<ulong> handlers = new List<ulong>();
            Console.WriteLine("*** Start time is " + startTime);
            //
            _allocatedEvaluatorHandler = new ClrSystemAllocatedEvaluatorHandler();
            ulong jhandler =HandlerHelper.Create_Handler(_allocatedEvaluatorHandler);
            handlers.Add(jhandler);
            Console.WriteLine("_allocatedEvaluatorHandler added");
            _allocatedEvaluatorHandler.Subscribe(new UserAllocatedEvaluatorHandler());
            //
            return handlers.ToArray();
        }

        public IDisposable Subscribe(IObserver<DateTime> observer)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnNext(DateTime value)
        {
            throw new NotImplementedException();
        }
    }
}
