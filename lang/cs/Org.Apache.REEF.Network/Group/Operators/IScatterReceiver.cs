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

using System.Collections.Generic;
using System.Threading;

namespace Org.Apache.REEF.Network.Group.Operators
{
    /// <summary>
    /// Group Communication operator used to receive a sublist of messages sent
    /// from the IScatterSender.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public interface IScatterReceiver<T> : IGroupCommOperator<T>
    {
        /// <summary>
        /// Receive a sublist of messages sent from the IScatterSender.
        /// </summary>
        /// <returns>The sublist of messages</returns>
        //// TODO : REEF-1489 to remove null
        List<T> Receive(CancellationTokenSource cancellationSource = null);
    }
}
