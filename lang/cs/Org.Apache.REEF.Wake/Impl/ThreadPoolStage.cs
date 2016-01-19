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

using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Util;

namespace Org.Apache.REEF.Wake.Impl
{
    /// <summary>Stage that executes an event handler with a thread pool</summary>
    public class ThreadPoolStage<T> : AbstractEStage<T>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ThreadPoolStage<T>));

        private readonly IEventHandler<T> _handler;

        private readonly ITaskService _taskService;

        private readonly int _numThreads;

        /// <summary>Constructs a thread-pool stage</summary>
        /// <param name="handler">An event handler to execute</param>
        /// <param name="numThreads">The number of threads to use</param>
        public ThreadPoolStage(IEventHandler<T> handler, int numThreads) 
            : base(handler.GetType().FullName)
        {
            _handler = handler;
            if (numThreads <= 0)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new WakeRuntimeException("numThreads " + numThreads + " is less than or equal to 0"), LOGGER);
            }
            _numThreads = numThreads;
            _taskService = new FixedThreadPoolTaskService(numThreads);
        }

        /// <summary>Constructs a thread-pool stage</summary>
        /// <param name="handler">an event handler to execute</param>
        /// <param name="taskService">an external executor service provided</param>
        public ThreadPoolStage(IEventHandler<T> handler, ITaskService taskService) : base(
            handler.GetType().FullName)
        {
            _handler = handler;
            _numThreads = 0;
            _taskService = taskService;
        }

        /// <summary>Handles the event using a thread in the thread pool</summary>
        /// <param name="value">an event</param>
        public override void OnNext(T value)
        {
            base.OnNext(value);
            _taskService.Execute(new _Startable_74(this, value).Start);
        }

        /// <summary>
        /// Closes resources
        /// </summary>
        public override void Dispose()
        {
            if (_numThreads > 0)
            {
                _taskService.Shutdown();
            }
        }

        private sealed class _Startable_74 : IStartable
        {
            private readonly ThreadPoolStage<T> _enclosing;
            private readonly T _value;

            public _Startable_74(ThreadPoolStage<T> enclosing, T value)
            {
                _enclosing = enclosing;
                _value = value;
            }

            public void Start()
            {
                _enclosing._handler.OnNext(_value);
            }
        }
    }
}
