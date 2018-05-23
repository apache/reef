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
using System.Collections.Generic;
using System.IO;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Bridge.Core.Common.Driver
{
    /// <summary>
    /// The interface for talking to the Java driver.
    /// </summary>
    internal interface IDriverServiceClient
    {
        /// <summary>
        /// Initiate shutdown.
        /// </summary>
        void OnShutdown();

        /// <summary>
        /// Initiate shutdown with exception.
        /// </summary>
        /// <param name="ex">exception</param>
        void OnShutdown(Exception ex);


        /// <summary>
        /// Set an alarm that will timeout it the given
        /// amount of milliseconds.
        /// </summary>
        /// <param name="alarmId">Identifies the alarm</param>
        /// <param name="timeoutMs">When to tigger the alarm in the future</param>
        void OnSetAlarm(string alarmId, long timeoutMs);


        /// <summary>
        /// Request evalautor resources.
        /// </summary>
        /// <param name="evaluatorRequest">Request details</param>
        void OnEvaluatorRequest(IEvaluatorRequest evaluatorRequest);

        /// <summary>
        /// Close the evaluator with the given identifier.
        /// </summary>
        /// <param name="evalautorId">Evaluator identifier</param>
        void OnEvaluatorClose(string evalautorId);

        /// <summary>
        /// Submit the evalautor with the given configuration.
        /// </summary>
        /// <param name="evaluatorId">Evaluator identifier</param>
        /// <param name="contextConfiguration">Context configuration</param>
        /// <param name="serviceConfiguration">Service configuration</param>
        /// <param name="taskConfiguration">Task configuration</param>
        /// <param name="addFileList">Files that should be included</param>
        /// <param name="addLibraryList">Libraries that should be included</param>
        void OnEvaluatorSubmit(
            string evaluatorId,
            IConfiguration contextConfiguration,
            Optional<IConfiguration> serviceConfiguration,
            Optional<IConfiguration> taskConfiguration,
            List<FileInfo> addFileList,
            List<FileInfo> addLibraryList);

        /// <summary>
        /// Close the context with the given identifier.
        /// </summary>
        /// <param name="contextId">Context identifier</param>
        void OnContextClose(string contextId);

        /// <summary>
        /// Submit a new context with the given configuration.
        /// </summary>
        /// <param name="contextId">Context identifier through which a new context will be created</param>
        /// <param name="contextConfiguration">Configuration of the new (child) context</param>
        void OnContextSubmitContext(string contextId, IConfiguration contextConfiguration);

        /// <summary>
        /// Submit a task via the given context.
        /// </summary>
        /// <param name="contextId">Context identifier</param>
        /// <param name="taskConfiguration">Task configuration</param>
        void OnContextSubmitTask(string contextId, IConfiguration taskConfiguration);

        /// <summary>
        /// Send a message to the given context.
        /// </summary>
        /// <param name="contextId">Context identifier</param>
        /// <param name="message">Message to send</param>
        void OnContextMessage(string contextId, byte[] message);

        /// <summary>
        /// Close the task with an optional message.
        /// </summary>
        /// <param name="taskId">Task identifier</param>
        /// <param name="message">Optional message</param>
        void OnTaskClose(string taskId, Optional<byte[]> message);

        /// <summary>
        /// Suspend the task with an optional message.
        /// </summary>
        /// <param name="taskId">Task identifier to suspend</param>
        /// <param name="message">Optional message</param>
        void OnTaskSuspend(string taskId, Optional<byte[]> message);

        /// <summary>
        /// Send a message to the task.
        /// </summary>
        /// <param name="taskId">Task identifier</param>
        /// <param name="message">Message to send</param>
        void OnTaskMessage(string taskId, byte[] message);
    }
}
