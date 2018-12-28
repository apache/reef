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
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Failures.Impl
{
    /// <summary>
    /// Reconfigure the execution to work with fewer tasks and simultaneusly try to
    /// reschedule a new task.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public class RescheduleEvent : IReschedule
    {
        /// <summary>
        /// Constructor for the reschedule event.
        /// </summary>
        /// <param name="taskId">The identifier of the task triggering the failure event</param>
        public RescheduleEvent(string taskId)
        {
            TaskId = taskId;
            OperatorId = -1;
            FailureResponse = new List<IElasticDriverMessage>();
            RescheduleTaskConfigurations = new Dictionary<string, IList<IConfiguration>>();
            Iteration = Optional<int>.Empty();
        }

        /// <summary>
        /// The event / action raised by the transition to the new failure state.
        /// </summary>
        public int FailureEvent
        {
            get { return (int)DefaultFailureStateEvents.Reschedule; }
        }

        /// <summary>
        /// The failed task triggering the event.
        /// </summary>
        public Optional<IFailedTask> FailedTask { get; set; }

        /// <summary>
        /// The identifier of the task triggering the event.
        /// </summary>
        public string TaskId { get; private set; }

        /// <summary>
        /// The opeartor id in which the failure is rised.
        /// </summary>
        public int OperatorId { get; private set; }

        /// <summary>
        /// The iteration in which the failure is rised.
        /// </summary>
        public Optional<int> Iteration { get; set; }

        /// <summary>
        /// Messages implementing the response from the driver to the tasks
        /// to reconfigure the compution.
        /// </summary>
        public List<IElasticDriverMessage> FailureResponse { get; private set; }

        /// <summary>
        /// The configurations for the stages of the task.
        /// </summary>
        public Dictionary<string, IList<IConfiguration>> RescheduleTaskConfigurations { get; private set; }

        /// <summary>
        /// Whether the task should be rescheduled as consequence of this event.
        /// </summary>
        public bool Reschedule
        {
            get { return RescheduleTaskConfigurations.Count > 0; }
        }
    }
}
