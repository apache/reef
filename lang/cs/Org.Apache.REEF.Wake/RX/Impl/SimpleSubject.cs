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

namespace Org.Apache.REEF.Wake.RX.Impl
{
    /// <summary>A Subject that relays all messages to its subscribers.</summary>
    public class SimpleSubject<T> : ISubject<T, T>
    {
        private readonly IObserver<T> _observer;

        /// <summary>Constructs a simple subject</summary>
        /// <param name="observer">the observer</param>
        public SimpleSubject(IObserver<T> observer)
        {
            _observer = observer;
        }

        /// <summary>Provides the observer with the new value</summary>
        /// <param name="value">the new value</param>
        public virtual void OnNext(T value)
        {
            _observer.OnNext(value);
        }

        /// <summary>Provides the observer with the error</summary>
        /// <param name="error">the error</param>
        public virtual void OnError(Exception error)
        {
            _observer.OnError(error);
        }

        /// <summary>
        /// Provides the observer with it has finished sending push-based
        /// notifications.
        /// </summary>
        public virtual void OnCompleted()
        {
            _observer.OnCompleted();
        }
    }
}
