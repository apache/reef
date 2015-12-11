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
using System.Collections.Generic;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Util;

namespace Org.Apache.REEF.Wake.Examples.P2p
{
    /// <summary>Performs a Pull-to-Push conversion in Wake.</summary>
    /// <remarks>
    /// Performs a Pull-to-Push conversion in Wake.
    /// The class pulls from a set of event sources, and pushes to a single
    /// EventHandler. If the downstream event handler blocks, this will block,
    /// providing a simple rate limiting scheme.
    /// The EventSources are managed in a basic Queue.
    /// </remarks>
    public sealed class Pull2Push<T> : IStartable, IDisposable
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(Pull2Push<T>));
        
        private readonly IEventHandler<T> _output;

        private readonly Queue<IEventSource<T>> _sources = new Queue<IEventSource<T>>();

        private bool _closed = false;

        /// <summary>
        /// Constructs a new Pull2Push object
        /// </summary>
        /// <param name="output">
        /// the EventHandler that receives the messages from this
        /// Pull2Push.
        /// </param>
        public Pull2Push(IEventHandler<T> output)
        {
            // The downstream EventHandler
            // The upstream event sources
            _output = output;
        }

        /// <summary>Registers an event source.</summary>
        /// <param name="source">
        /// The source that will be added to the queue of this
        /// Pull2Push
        /// </param>
        public void Register(IEventSource<T> source)
        {
            _sources.Enqueue(source);
        }

        /// <summary>Executes the message loop.</summary>
        public void Start()
        {
            while (!_closed)
            {
                // Grab the next available message source, if any
                IEventSource<T> nextSource = _sources.Dequeue();
                if (null != nextSource)
                {
                    // Grab the next message from that source, if any
                    T message = nextSource.GetNext();
                    if (null != message)
                    {
                        // Add the source to the end of the queue again.
                        _sources.Enqueue(nextSource);

                        // Send the message. Note that this may block depending on the underlying EventHandler.
                        _output.OnNext(message);
                    }
                    else
                    {
                        // The message source has returned null as the next message. We drop the message source in that case.
                        LOGGER.Log(Level.Info, "Dropping message source {0} from the queue " + nextSource.ToString());
                    }
                }
            }
        }

        // No source where available. We could put a wait() here. 
        public void Dispose()
        {
            _closed = true;
        }
    }
}
