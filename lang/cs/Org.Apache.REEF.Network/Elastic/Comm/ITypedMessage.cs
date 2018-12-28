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

namespace Org.Apache.REEF.Network.Elastic.Comm
{
    /// <summary>
    /// Typed interface for data messages.
    /// This is used to provide a unified interface over the
    /// different types of data messages.
    /// </summary>
    /// <typeparam name="T">The ty</typeparam>
    internal interface ITypedDataMessage<T>
    {
        /// <summary>
        /// The data contained in the message.
        /// </summary>
        T Data { get; }

        /// <summary>
        /// The iteration number for the message.
        /// </summary>
        int Iteration { get; }
    }
}
