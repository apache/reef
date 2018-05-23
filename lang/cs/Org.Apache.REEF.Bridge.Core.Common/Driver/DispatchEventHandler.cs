// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Collections.Generic;

namespace Org.Apache.REEF.Bridge.Core.Common.Driver
{
    internal sealed class DispatchEventHandler<T> : IObserver<T>, IObservable<T>
    {
        private readonly ISet<IObserver<T>> _userHandlers;

        public DispatchEventHandler()
        {
            _userHandlers = new HashSet<IObserver<T>>();
        }

        public DispatchEventHandler(ISet<IObserver<T>> userHandlers)
        {
            _userHandlers = userHandlers;
        }

        public void OnNext(T value)
        {
            foreach (var observer in _userHandlers)
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
            _userHandlers.Add(observer);
            return new DisposeImpl()
            {
                DispatchEventHandler = this,
                Handler = observer
            };
        }

        private class DisposeImpl : IDisposable
        {
            public DispatchEventHandler<T> DispatchEventHandler { private get; set; }
            public IObserver<T> Handler { private get; set; }

            public void Dispose()
            {
                DispatchEventHandler._userHandlers.Remove(Handler);
            }
        }
    }
}