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

using Org.Apache.REEF.Common.Runtime.Evaluator;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Attributes;
using static Org.Apache.REEF.Common.Protobuf.ReefProtocol.TaskStatusProto;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    /// <summary>
    /// Class used to manage messages going from tasks to the driver.
    /// Messages are notifying through the heartbeat.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal abstract class TaskToDriverMessageDispatcher
    {
        private readonly IHeartBeatManager _heartBeatManager;

        /// <summary>
        /// Constrcutor.
        /// </summary>
        /// <param name="heartBeatManager">Reference to the heartbeat manager</param>
        protected TaskToDriverMessageDispatcher(IHeartBeatManager heartBeatManager)
        {
            _heartBeatManager = heartBeatManager;
        }

        /// <summary>
        /// Send a serialized message to the driver.
        /// </summary>
        /// <param name="taskId">The id of the task sending the message</param>
        /// <param name="message">The serizlied message to send</param>
        protected void Send(string taskId, byte[] message)
        {
            TaskStatusProto taskStatusProto = new TaskStatusProto()
            {
                task_id = taskId,
                context_id = Utils.GetContextIdFromTaskId(taskId),
                task_message = { new TaskMessageProto { source_id = taskId, message = message } }
            };

            Heartbeat(taskStatusProto);
        }

        private void Heartbeat(TaskStatusProto proto)
        {
            var state = _heartBeatManager.ContextManager.GetTaskStatus();

            if (state.IsPresent())
            {
                proto.state = state.Value.state;
            }

            _heartBeatManager.OnNext(proto);
        }
    }
}
