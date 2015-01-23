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
 
using Org.Apache.Reef.Wake;

namespace Org.Apache.Reef.Wake.Impl
{
    /// <summary>Stage that synchronously executes an event handler</summary>
    public class SyncStage<T> : AbstractEStage<T>
    {
        private readonly IEventHandler<T> _handler;

        /// <summary>Constructs a synchronous stage</summary>
        /// <param name="handler">an event handler</param>
        public SyncStage(IEventHandler<T> handler) : base(handler.GetType().FullName)
        {
            _handler = handler;
        }

        /// <summary>Invokes the handler for the event</summary>
        /// <param name="value">an event</param>
        public override void OnNext(T value)
        {
            base.OnNext(value);
            _handler.OnNext(value);
        }

        public override void Dispose()
        {
        }
    }
}
