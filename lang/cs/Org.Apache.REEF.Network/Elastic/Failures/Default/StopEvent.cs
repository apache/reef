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

using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Utilities.Attributes;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Failures.Default
{
    /// <summary>
    /// Stop the execution and try to add new tasks.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public class StopEvent : IFailureEvent
    {
        /// <summary>
        /// Constructor for the stop event.
        /// </summary>
        /// <param name="taskId">The identifier of the task triggering the failure event</param>
        public StopEvent(string taskId)
        {
            TaskId = taskId;
            OperatorId = -1;
            FailureResponse = new List<IElasticDriverMessage>();
        }

        /// <summary>
        /// The event / action raised by the transition to the new failure state.
        /// </summary>
        public int FailureEvent
        {
            get { return (int)DefaultFailureStateEvents.Stop; }
        }

        /// <summary>
        /// The identifier of the task triggering the event.
        /// </summary>
        public string TaskId { get; private set; }

        /// <summary>
        /// The opeartor id in which the failure is rised.
        /// </summary>
        public int OperatorId { get; private set; }

        /// <summary>
        /// The response message generated to react to the failure event.
        /// </summary>
        public List<IElasticDriverMessage> FailureResponse { get; private set; }
    }
}
