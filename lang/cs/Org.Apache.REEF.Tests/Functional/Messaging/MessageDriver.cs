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
using System.Globalization;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using IRunningTask = Org.Apache.REEF.Driver.Task.IRunningTask;

namespace Org.Apache.REEF.Tests.Functional.Messaging
{
    public class MessageDriver :
        IObserver<IAllocatedEvaluator>, 
        IObserver<ITaskMessage>, 
        IObserver<IRunningTask>, 
        IObserver<IDriverStarted>
    {
        public const int NumberOfEvaluator = 1;

        public const string Message = "MESSAGE::DRIVER";

        private static readonly Logger LOGGER = Logger.GetLogger(typeof(MessageDriver));
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        [Inject]
        public MessageDriver(IEvaluatorRequestor evaluatorRequestor)
        {
            _evaluatorRequestor = evaluatorRequestor;
        }
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

        public void OnNext(ITaskMessage taskMessage)
        {
            string msgReceived = ByteUtilities.ByteArraysToString(taskMessage.Message);

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

        public void OnNext(IDriverStarted value)
        {
            var request =
                _evaluatorRequestor.NewBuilder()
                    .SetNumber(NumberOfEvaluator)
                    .SetMegabytes(512)
                    .SetCores(2)
                    .SetRackName("WonderlandRack")
                    .SetEvaluatorBatchId("TaskMessagingEvaluator")
                    .Build();
            _evaluatorRequestor.Submit(request);
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }
    }
}