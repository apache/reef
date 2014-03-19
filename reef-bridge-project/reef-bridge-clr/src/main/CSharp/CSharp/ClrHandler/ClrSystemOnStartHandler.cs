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
        public static ulong OnStart(DateTime startTime)
        {

            Console.WriteLine("*** Start time is " + startTime);
            //
            _allocatedEvaluatorHandler = new ClrSystemAllocatedEvaluatorHandler();
            ulong ul = ClrSystemAllocatedEvaluatorHandlerWrapper.CreateFromString_ClrSystemAllocatedEvaluatorHandler(_allocatedEvaluatorHandler);
            _allocatedEvaluatorHandler.Subscribe(new UserAllocatedEvaluatorHandler());
            //
            return ul;
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
