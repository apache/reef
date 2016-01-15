/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Org.Apache.REEF.Wake.Impl
{
    /// <summary>Single thread stage that runs the event handler</summary>
    public class SingleThreadStage<T> : AbstractEStage<T>
    {
        private readonly BlockingCollection<T> queue;

        private readonly Thread thread;

        private bool interrupted;

        public SingleThreadStage(IEventHandler<T> handler, int capacity) : base(handler.GetType().FullName)
        {
            queue = new BlockingCollection<T>(capacity);
            interrupted = false;
            thread = new Thread(new Producer(queue, handler, interrupted).Run);
            thread.Start();
        }

        /// <summary>
        /// Puts the value to the queue, which will be processed by the handler later
        /// if the queue is full, IllegalStateException is thrown
        /// </summary>
        /// <param name="value">the value</param>
        public override void OnNext(T value)
        {
            base.OnNext(value);
            queue.Add(value);
        }

        /// <summary>
        /// Closes the stage
        /// </summary>
        public override void Dispose()
        {
            interrupted = true;
            thread.Interrupt();
        }

        /// <summary>Takes events from the queue and provides them to the handler</summary>
        private sealed class Producer
        {
            private readonly BlockingCollection<T> _queue;

            private readonly IEventHandler<T> _handler;

            private volatile bool _interrupted;

            internal Producer(BlockingCollection<T> queue, IEventHandler<T> handler, bool interrupted)
            {
                _queue = queue;
                _handler = handler;
                _interrupted = interrupted;
            }

            public void Run()
            {
                while (true)
                {
                    try
                    {
                        T value = _queue.Take();
                        _handler.OnNext(value);
                    }
                    catch (Exception)
                    {
                        if (_interrupted)
                        {
                            break;
                        }
                    }
                }
            }
        }
    }
}
