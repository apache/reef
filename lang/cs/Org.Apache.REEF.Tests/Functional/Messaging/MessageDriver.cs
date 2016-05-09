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
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using IRunningTask = Org.Apache.REEF.Driver.Task.IRunningTask;

namespace Org.Apache.REEF.Tests.Functional.Messaging
{
    public sealed class MessageDriver :
        IObserver<IAllocatedEvaluator>, 
        IObserver<ITaskMessage>, 
        IObserver<IContextMessage>,
        IObserver<IRunningTask>, 
        IObserver<IDriverStarted>,
        IObserver<IActiveContext>
    {
        public const int NumberOfEvaluator = 1;

        public const string Message = "MESSAGE::DRIVER";

        public const string SendingMessageToTaskLog = "Sending message to Task.";
        public const string DriverReceivedTaskMessageLog = "Driver received message from Task.";

        public const string SendingMessageToContextLog = "Sending message to Context.";
        public const string DriverReceivedContextMessageLog = "Driver received message from Context.";

        private static readonly Logger Logger = Logger.GetLogger(typeof(MessageDriver));
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        [Inject]
        public MessageDriver(IEvaluatorRequestor evaluatorRequestor)
        {
            _evaluatorRequestor = evaluatorRequestor;
        }

        public void OnNext(IAllocatedEvaluator eval)
        {
            const string contextId = "ContextID";

            var serviceConfiguration = ServiceConfiguration.ConfigurationModule
                .Set(ServiceConfiguration.Services, GenericType<MessageManager>.Class)
                .Build();

            var contextConfiguration = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, contextId)
                .Set(ContextConfiguration.OnSendMessage, GenericType<MessageContext>.Class)
                .Set(ContextConfiguration.OnMessage, GenericType<MessageContext>.Class)
                .Build();

            eval.SubmitContextAndService(contextConfiguration, serviceConfiguration);
        }

        public void OnNext(ITaskMessage taskMessage)
        {
            // TODO[JIRA REEF-1385]: Check the MessageTaskSourceID.
            var msgReceived = ByteUtilities.ByteArraysToString(taskMessage.Message);

            if (!msgReceived.Equals(MessageTask.MessageSend))
            {
                Exceptions.Throw(new Exception("Unexpected message: " + msgReceived),
                    "Unexpected task message received: " + msgReceived,
                    Logger);
            }
            else
            {
                Logger.Log(Level.Info, DriverReceivedTaskMessageLog);
            }
        }

        public void OnNext(IContextMessage contextMessage)
        {
            var msgReceived = ByteUtilities.ByteArraysToString(contextMessage.Message);

            if (!msgReceived.Equals(MessageContext.MessageSend))
            {
                Exceptions.Throw(new Exception("Expected message: " + MessageContext.MessageSend),
                    "Unxpected context message received: " + msgReceived,
                    Logger);
            }
            else if (!contextMessage.MessageSourceId.Equals(MessageContext.MessageSourceID))
            {
                Exceptions.Throw(new Exception("Expected Context MessageSourceID: " + MessageContext.MessageSourceID),
                    "Unexpected context MessageSourceID received: " + contextMessage.MessageSourceId,
                    Logger);
            }
            else
            {
                Logger.Log(Level.Info, DriverReceivedContextMessageLog);
            }
        }

        public void OnNext(IRunningTask runningTask)
        {
            Logger.Log(Level.Info, SendingMessageToTaskLog);
            runningTask.Send(ByteUtilities.StringToByteArrays(Message));
        }

        public void OnNext(IActiveContext activeContext)
        {
            const string taskId = "TaskID";

            Logger.Log(Level.Info, SendingMessageToContextLog);
            activeContext.SendMessage(ByteUtilities.StringToByteArrays(Message));

            var taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, taskId)
                .Set(TaskConfiguration.Task, GenericType<MessageTask>.Class)
                .Set(TaskConfiguration.OnMessage, GenericType<MessageTask>.Class)
                .Set(TaskConfiguration.OnSendMessage, GenericType<MessageTask>.Class)
                .Build();
            activeContext.SubmitTask(taskConfiguration);
        }

        public void OnNext(IDriverStarted value)
        {
            var request =
                _evaluatorRequestor.NewBuilder()
                    .SetNumber(NumberOfEvaluator)
                    .SetMegabytes(512)
                    .SetCores(2)
                    .SetRackName("WonderlandRack")
                    .SetEvaluatorBatchId("MessagingEvaluator")
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