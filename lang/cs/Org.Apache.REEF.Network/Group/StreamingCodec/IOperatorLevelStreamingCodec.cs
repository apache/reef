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

using System.Collections.Generic;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Network.Group.StreaminngCodec
{

    /// <summary>
    /// This extended interface is needed at the actual operator level.
    /// IStreamingCodec does not depend on type T and hence is good for lower layers like WAKE
    /// At operator level we need to access the byte message written to IStreamingCodec as actual
    /// message of Type T.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public interface IOperatorLevelStreamingCodec<T> : IStreamingCodec
    {
        /// <summary>
        /// gives actual communicated message
        /// </summary>
        /// <returns>Communicated message</returns>
        T StreamingDecoder();

        /// <summary>
        /// feeds the message to be communicated to the codec
        /// </summary>
        /// <param name="data">message to be encoded</param>
        void FeedMessage(T data);
    }
}
