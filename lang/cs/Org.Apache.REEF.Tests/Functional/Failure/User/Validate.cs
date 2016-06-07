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
using Org.Apache.REEF.Common.Runtime.Evaluator.Task;
using Org.Apache.REEF.Driver.Task;

namespace Org.Apache.REEF.Tests.Functional.Failure.User
{
    /// <summary>
    /// A helper class to help validate user errors.
    /// </summary>
    internal static class Validate
    {
        /// <summary>
        /// Validates that the failed task has a <see cref="TaskClientCodeException"/> and has
        /// the expected ContextId, TaskId, and contains the right error message.
        /// Throws an <see cref="Exception"/> if false.
        /// </summary>
        internal static void ValidateTaskClientCodeException(
            IFailedTask failedTask,
            string expectedContextId = null,
            string expectedTaskId = null,
            string expectedErrorMessage = null)
        {
            var taskClientCodeEx = failedTask.AsError() as TaskClientCodeException;
            if (taskClientCodeEx == null)
            {
                throw new Exception("Expected Exception to be a TaskClientCodeException.");
            }

            if (!string.IsNullOrWhiteSpace(expectedContextId) &&
                taskClientCodeEx.ContextId != expectedContextId)
            {
                throw new Exception("Expected Context ID to be " + expectedContextId + ", but instead got " + taskClientCodeEx.ContextId);
            }

            if (!string.IsNullOrWhiteSpace(expectedTaskId) &&
                taskClientCodeEx.TaskId != expectedTaskId)
            {
                throw new Exception("Expected Task ID to be " + expectedTaskId + ", but instead got " + taskClientCodeEx.TaskId);
            }

            if (!string.IsNullOrWhiteSpace(expectedErrorMessage))
            {
                Exception error = taskClientCodeEx;

                var foundErrorMessage = false;
                while (error != null)
                {
                    // Using Contains because the Exception may not be serializable
                    // and the message of the wrapping Exception may be expanded to include more details.
                    if (error.Message.Contains(expectedErrorMessage))
                    {
                        foundErrorMessage = true;
                        break;
                    }

                    error = error.InnerException;
                }

                if (!foundErrorMessage)
                {
                    throw new Exception("Expected to find error message " + expectedErrorMessage + " in the layer of Exceptions.");
                }
            }
        }
    }
}