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
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Context
{
    public sealed class ContextMessage
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ContextMessage));
        private readonly string _messageSourcId;
        private readonly byte[] _bytes;

        private ContextMessage(string messageSourceId, byte[] bytes)
        {
            _messageSourcId = messageSourceId;
            _bytes = bytes;
        }

        public string MessageSourceId
        {
            get { return _messageSourcId; }
        }

        public byte[] Bytes
        {
            get { return _bytes; }
        }

        /// <summary>
        /// construt a new new ContextMessage with the given content.
        /// </summary>
        /// <param name="messageSourceId">The message's sourceID. This will be accessible in the Driver for routing.</param>
        /// <param name="bytes">The actual content of the message, serialized into a byte[]</param>
        /// <returns>new ContextMessage with the given content.</returns>
        public static ContextMessage From(string messageSourceId, byte[] bytes)
        {
            if (string.IsNullOrEmpty(messageSourceId))
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ArgumentNullException("messageSourceId"), LOGGER);
            }
            if (bytes == null)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ArgumentNullException("bytes"), LOGGER);
            }
            return new ContextMessage(messageSourceId, bytes);
        }
    }
}
