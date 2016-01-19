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

namespace Org.Apache.REEF.Network.Group.Operators
{
    /// <summary>
    /// Group Communication operator used to scatter a list of elements to all
    /// of the IScatterReceivers.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public interface IScatterSender<T> : IGroupCommOperator<T>
    {
        /// <summary>
        /// Split up the list of elements evenly and scatter each chunk
        /// to the IScatterReceivers.
        /// </summary>
        /// <param name="elements">The list of elements to send.</param>
        void Send(List<T> elements);

        /// <summary>
        /// Split up the list of elements and scatter each chunk
        /// to the IScatterReceivers.  Each receiver will receive
        /// a sublist of the specified size.
        /// </summary>
        /// <param name="elements">The list of elements to send.</param>
        /// <param name="count">The size of each sublist</param>
        void Send(List<T> elements, int count);

        /// <summary>
        /// Split up the list of elements and scatter each chunk
        /// to the IScatterReceivers in the specified task order.
        /// </summary>
        /// <param name="elements">The list of elements to send.</param>
        /// <param name="order">The list of task identifiers representing
        /// the order in which to scatter each sublist</param>
        void Send(List<T> elements, List<string> order);
    }
}
