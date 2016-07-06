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

using System.Threading;

namespace Org.Apache.REEF.Network.Group.Operators
{
    /// <summary>
    /// Group Communication Operator used to send messages to be reduced by the ReduceReceiver.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public interface IReduceSender<T> : IGroupCommOperator<T>
    {
        /// <summary>
        /// Get reduced data from children, reduce with the data given, then sends reduced data to parent
        /// </summary>
        /// <param name="data">The data to send</param>
        /// <param name="cancellationSource">The cancellation token for the data reading operation cancellation</param>
        void Send(T data, CancellationTokenSource cancellationSource = null);
    }
}
