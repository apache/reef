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
using System.Runtime.Serialization;
using System.Security.Permissions;
using Org.Apache.REEF.Common.Exceptions;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Task
{
    /// <summary>
    /// An Exception thrown when Task operations (Start, Stop, Suspend) fail.
    /// </summary>
    [Serializable]
    public sealed class TaskClientCodeException : Exception
    {
        private const string TaskIdStr = "TaskId";
        private const string ContextIdStr = "ContextId";

        private readonly string _contextId;
        private readonly string _taskId;

        internal static TaskClientCodeException Create(
            string taskId,
            string contextId,
            string message,
            Exception cause)
        {
            return new TaskClientCodeException(taskId, contextId, message, cause);
        }

        internal static TaskClientCodeException CreateWithNonSerializableInnerException(
            TaskClientCodeException e, SerializationException serializationException)
        {
            var nonSerializableTaskException = NonSerializableTaskException.UnableToSerialize(e.InnerException, serializationException);

            return new TaskClientCodeException(
                e.TaskId, 
                e.ContextId, 
                string.Format("Unable to serialize Task control message. TaskClientCodeException message: {0}", e.Message), 
                nonSerializableTaskException);
        }

        /// <summary>
        /// construct the exception that caused the Task to fail
        /// </summary>
        /// <param name="taskId"> the id of the failed task.</param>
        /// <param name="contextId"> the id of the context the failed Task was executing in.</param>
        /// <param name="message"> the error message </param>
        /// <param name="cause"> the exception that caused the Task to fail.</param>
        private TaskClientCodeException(
                string taskId,
                string contextId,
                string message,
                Exception cause)
            : base(message, cause)
        {
            _taskId = taskId;
            _contextId = contextId;
        }

        /// <summary>
        /// construct the exception that caused the Task to fail
        /// </summary>
        /// <param name="taskId"> the id of the failed task.</param>
        /// <param name="contextId"> the id of the context the failed Task was executing in.</param>
        /// <param name="message"> the error message </param>
        private TaskClientCodeException(
                string taskId,
                string contextId,
                string message)
            : base(message)
        {
            _taskId = taskId;
            _contextId = contextId;
        }

        /// <summary>
        /// Constructor used for serialization.
        /// </summary>
        private TaskClientCodeException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            _taskId = info.GetString(TaskIdStr);
            _contextId = info.GetString(ContextIdStr);
        }

        public string TaskId
        {
            get { return _taskId; }
        }

        public string ContextId
        {
            get { return _contextId; }
        }

        [SecurityPermission(SecurityAction.Demand, SerializationFormatter = true)]
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new ArgumentNullException("info");
            }

            info.AddValue(TaskIdStr, TaskId);
            info.AddValue(ContextIdStr, ContextId);
            base.GetObjectData(info, context);
        }
    }
}
