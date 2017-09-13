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

namespace Org.Apache.REEF.Examples.PeerToPeer.Communication
{
    /// <summary>
    /// Message.
    /// </summary>
    internal sealed class Message<T>
    {
        /// <summary>
        /// Create new Message.
        /// </summary>
        /// <param name="source">The message source</param>
        /// <param name="destination">The message destination</param>
        /// <param name="message">The actual Writable message</param>
        internal Message(
            string source,
            string destination,
            T message)
        {
            Source = source;
            Destination = destination;
            Data = message;
        }

        /// <summary>
        /// Returns the Source of the message.
        /// </summary>
        internal string Source { get; private set; }

        /// <summary>
        /// Returns the Destination of the message.
        /// </summary>
        internal string Destination { get; private set; }

        /// <summary>
        /// Returns the payload of the message.
        /// </summary>
        internal T Data { get; private set; }
    }
}