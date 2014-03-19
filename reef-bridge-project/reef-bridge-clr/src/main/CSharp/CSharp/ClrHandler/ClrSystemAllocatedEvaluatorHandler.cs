using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ClrHandler;

namespace Microsoft.Reef.Interop
{
    public class ClrSystemAllocatedEvaluatorHandler : IObserver<AllocatedEvaluator>, IObservable<AllocatedEvaluator>
    {
        List<IObserver<AllocatedEvaluator>> userAllocatedEvaluatorHandlers = new List<IObserver<AllocatedEvaluator>>(); 

        public void OnNext(AllocatedEvaluator value)
        {
            foreach (var observer in userAllocatedEvaluatorHandlers)
            {
                observer.OnNext(value);
            }
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public IDisposable Subscribe(IObserver<AllocatedEvaluator> observer)
        {
            userAllocatedEvaluatorHandlers.Add(observer);
            return null;
        }
    }
}
