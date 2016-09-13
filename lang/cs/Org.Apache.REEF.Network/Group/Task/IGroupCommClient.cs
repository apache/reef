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

using System;
using System.Threading;
using Org.Apache.REEF.Network.Group.Task.Impl;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Network.Group.Task
{
    /// <summary>
    /// Used by Tasks to fetch CommunicationGroupClients.
    /// </summary>
    [DefaultImplementation(typeof(GroupCommClient))]
    public interface IGroupCommClient : IDisposable
    {
        /// <summary>
        /// Gets the CommunicationGroupClient with the given group name.
        /// </summary>
        /// <param name="groupName">The name of the CommunicationGroupClient</param>
        /// <returns>The configured CommunicationGroupClient</returns>
        ICommunicationGroupClient GetCommunicationGroup(string groupName);

        /// <summary>
        /// Initialization for group communications
        /// </summary>
        /// <param name="cancellationSource"></param>
        void Initialize(CancellationTokenSource cancellationSource = null);
    }
}
