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
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Util;

namespace Org.Apache.REEF.Wake.RX.Impl
{
    /// <summary>Stage that executes the observer with a thread pool</summary>
    public class RxThreadPoolStage<T> : AbstractRxStage<T>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(RxThreadPoolStage<T>));

        private readonly IObserver<T> _observer;

        private readonly ITaskService _taskService;

        /// <summary>Constructs a Rx thread pool stage</summary>
        /// <param name="observer">the observer to execute</param>
        /// <param name="numThreads">the number of threads</param>
        public RxThreadPoolStage(IObserver<T> observer, int numThreads) 
            : base(observer.GetType().FullName)
        {
            _observer = observer;
            if (numThreads <= 0)
            {
                Exceptions.Throw(new WakeRuntimeException("numThreads " + numThreads + " is less than or equal to 0"), LOGGER);
            }
            _taskService = new FixedThreadPoolTaskService(numThreads);
        }

        /// <summary>Provides the observer with the new value</summary>
        /// <param name="value">the new value</param>
        public override void OnNext(T value)
        {
            base.OnNext(value);
            _taskService.Execute(new _Startable_58(this, value).Start);
        }

        /// <summary>
        /// Notifies the observer that the provider has experienced an error
        /// condition.
        /// </summary>
        /// <param name="error">the error</param>
        public override void OnError(Exception error)
        {
            _taskService.Execute(new _Startable_75(this, error).Start);
        }

        /// <summary>
        /// Notifies the observer that the provider has finished sending push-based
        /// notifications.
        /// </summary>
        public override void OnCompleted()
        {
            _taskService.Execute(new _Startable_91(this).Start);
        }

        /// <summary>
        /// Closes the stage
        /// </summary>
        public override void Dispose()
        {
            _taskService.Shutdown();
        }

        private sealed class _Startable_58 : IStartable
        {
            private readonly RxThreadPoolStage<T> _enclosing;
            private readonly T _value;

            public _Startable_58(RxThreadPoolStage<T> enclosing, T value)
            {
                _enclosing = enclosing;
                _value = value;
            }

            public void Start()
            {
                _enclosing._observer.OnNext(_value);
            }
        }

        private sealed class _Startable_75 : IStartable
        {
            private readonly RxThreadPoolStage<T> _enclosing;
            private readonly Exception _error;

            public _Startable_75(RxThreadPoolStage<T> enclosing, Exception error)
            {
                _enclosing = enclosing;
                _error = error;
            }

            public void Start()
            {
                _enclosing._observer.OnError(_error);
            }
        }

        private sealed class _Startable_91 : IStartable
        {
            private readonly RxThreadPoolStage<T> _enclosing;

            public _Startable_91(RxThreadPoolStage<T> enclosing)
            {
                _enclosing = enclosing;
            }

            public void Start()
            {
                _enclosing._observer.OnCompleted();
            }
        }
    }
}
