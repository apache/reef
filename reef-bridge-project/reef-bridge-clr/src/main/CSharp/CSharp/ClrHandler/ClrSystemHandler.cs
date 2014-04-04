using System;
using System.Collections.Generic;

namespace Microsoft.Reef.Interop
{
    public class ClrSystemHandler<T> : IObserver<T>, IObservable<T>
    {
        List<IObserver<T>> userHandlers = new List<IObserver<T>>(); 

        public void OnNext(T value)
        {
            foreach (var observer in userHandlers)
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

        public IDisposable Subscribe(IObserver<T> observer)
        {
            userHandlers.Add(observer);
            return null;
        }
    }
}
