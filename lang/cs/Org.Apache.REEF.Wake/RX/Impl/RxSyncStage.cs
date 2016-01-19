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
    /// <summary>Stage that executes the observer synchronously</summary>
    public class RxSyncStage<T> : AbstractRxStage<T>
    {
        private readonly IObserver<T> _observer;

        /// <summary>Constructs a Rx synchronous stage</summary>
        /// <param name="observer">the observer</param>
        public RxSyncStage(IObserver<T> observer) : base(observer.GetType().FullName)
        {
            _observer = observer;
        }

        /// <summary>Provides the observer with the new value</summary>
        /// <param name="value">the new value</param>
        public override void OnNext(T value)
        {
            base.OnNext(value);
            _observer.OnNext(value);
        }

        /// <summary>
        /// Notifies the observer that the provider has experienced an error
        /// condition.
        /// </summary>
        /// <param name="error">the error</param>
        public override void OnError(Exception error)
        {
            _observer.OnError(error);
        }

        /// <summary>
        /// Notifies the observer that the provider has finished sending push-based
        /// notifications.
        /// </summary>
        public override void OnCompleted()
        {
            _observer.OnCompleted();
        }

        /// <summary>
        /// Closes the stage
        /// </summary>
        public override void Dispose()
        {
        }
    }
}
