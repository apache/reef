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

using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    /// <summary>
    /// Default failures response interface.
    /// The default events are Reconfigure, Reschedule, Stop and Fail.
    /// Mechanisms implementing the default failure responses must extend this interface.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public interface IDefaultFailureEventResponse
    {
        /// <summary>
        /// Mechanism to execute when a reconfigure event is triggered.
        /// <paramref name="reconfigureEvent"/>
        /// </summary>
        void OnReconfigure(ref IReconfigure reconfigureEvent);

        /// <summary>
        /// Mechanism to execute when a reschedule event is triggered.
        /// <paramref name="rescheduleEvent"/>
        /// </summary>
        void OnReschedule(ref IReschedule rescheduleEvent);

        /// <summary>
        /// Mechanism to execute when a stop event is triggered.
        /// <paramref name="stopEvent"/>
        /// </summary>
        void OnStop(ref IStop stopEvent);

        /// <summary>
        /// Mechanism to execute when a fail event is triggered.
        /// </summary>
        void OnFail();
    }
}
