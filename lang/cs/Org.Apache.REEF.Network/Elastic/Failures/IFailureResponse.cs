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

using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    /// <summary>
    /// Entry point for classes expected to be aware and act over failres.
    /// Used to propagate failures through operators, subscriptions and the service.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public interface IFailureResponse
    {
        /// <summary>
        /// Used to react on a failure occurred on a task.
        /// </summary>
        /// <param name="info">The failed task</param>
        /// <returns>The failure state after the notification of the failed task</returns>
        IFailureState OnTaskFailure(IFailedTask task);

        /// <summary>
        /// When a new failure state is rised, this method is used to dispatch
        /// such event to the proper failure mitigation logic.
        /// </summary>
        /// <param name="event">Notification specifiying the updated failure state</param>
        void EventDispatcher(IFailureEvent @event);
    }
}