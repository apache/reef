/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Text;
using Org.Apache.Reef.Driver;
using Org.Apache.Reef.Driver.Bridge;
using Org.Apache.Reef.Driver.Context;
using Org.Apache.Reef.Driver.Evaluator;
using Org.Apache.Reef.Driver.Task;
using Org.Apache.Reef.IO.Network.Naming;
using Org.Apache.Reef.Services;
using Org.Apache.Reef.Tasks;
using Org.Apache.Reef.Utilities;
using Org.Apache.Reef.Utilities.Diagnostics;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Tang.Implementations.Configuration;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Tang.Util;

namespace Org.Apache.Reef.Test
{
    public class MessageDriver : IStartHandler, IObserver<IAllocatedEvaluator>, IObserver<IEvaluatorRequestor>, IObserver<ITaskMessage>, IObserver<IRunningTask>
    {
        public const int NumerOfEvaluator = 1;

        public const string Message = "MESSAGE::DRIVER";

        private static readonly Logger LOGGER = Logger.GetLogger(typeof(MessageDriver));

        [Inject]
        public MessageDriver()
        {
            CreateClassHierarchy();
            Identifier = "TaskMessagingStartHandler";
        }

        public string Identifier { get; set; }

        public void OnNext(IAllocatedEvaluator eval)
        {
            string taskId = "Task_" + eval.Id;

            IConfiguration contextConfiguration = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, taskId)
                .Build();

            IConfiguration taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, taskId)
                .Set(TaskConfiguration.Task, GenericType<MessageTask>.Class)
                .Set(TaskConfiguration.OnMessage, GenericType<MessageTask.MessagingDriverMessageHandler>.Class)
                .Set(TaskConfiguration.OnSendMessage, GenericType<MessageTask>.Class)
                .Build();

            eval.SubmitContextAndTask(contextConfiguration, taskConfiguration);
        }

        public void OnNext(IEvaluatorRequestor evalutorRequestor)
        {
            EvaluatorRequest request = new EvaluatorRequest(NumerOfEvaluator, 512, 2, "WonderlandRack", "TaskMessagingEvaluator");
            evalutorRequestor.Submit(request);
        }

        public void OnNext(ITaskMessage taskMessage)
        {
            string msgReceived = ByteUtilities.ByteArrarysToString(taskMessage.Message);

            LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "CLR TaskMessagingTaskMessageHandler received following message from Task: {0}, Message: {1}.", taskMessage.TaskId, msgReceived));

            if (!msgReceived.StartsWith(MessageTask.MessageSend, true, CultureInfo.CurrentCulture))
            {
                Exceptions.Throw(new Exception("Unexpected message: " + msgReceived), "Unexpected task message received: " + msgReceived, LOGGER);
            }
        }

        public void OnNext(IRunningTask runningTask)
        {
            LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "TaskMessegingRunningTaskHandler: {0} is to send message {1}.", runningTask.Id, Message));
            runningTask.Send(ByteUtilities.StringToByteArrays(Message));
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        private void CreateClassHierarchy()
        {
            HashSet<string> clrDlls = new HashSet<string>();
            clrDlls.Add(typeof(IDriver).Assembly.GetName().Name);
            clrDlls.Add(typeof(ITask).Assembly.GetName().Name);
            clrDlls.Add(typeof(MessageTask).Assembly.GetName().Name);

            ClrHandlerHelper.GenerateClassHierarchy(clrDlls);
        }
    }
}