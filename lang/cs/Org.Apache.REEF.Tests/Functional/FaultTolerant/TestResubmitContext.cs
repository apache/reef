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
using System.Text;
using System.Threading;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.FaultTolerant
{
    [Collection("FunctionalTests")]
    public sealed class TestResubmitContext : ReefFunctionalTest
    {
        private const string FailedEvaluatorMessage = "FailedEvaluatorMessage";
        private const string CompletedTaskValidationMessage = "CompletedTaskValidationmessage";
        private const string CompletedEvaluatorValidationMessage = "CompletedEvaluatorValidationMessage";
        private const string FailSignal = "Fail";
        private const string SuccSignal = "Succ";
        private const string TaskId = "TaskId";

        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test invocation of ResubmitEvaluatorDriver")]
        public void ResubmitContextOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfigurations(), typeof(ResubmitEvaluatorDriver), 3, "ResubmitContextOnLocalRuntime", "local", testFolder);
            
            ValidateSuccessForLocalRuntime(2, numberOfEvaluatorsToFail: 1, testFolder: testFolder);

            var messages = new List<string>();
            messages.Add(FailedEvaluatorMessage);
            ValidateMessageSuccessfullyLogged(messages, "driver", DriverStdout, testFolder, 1);

            var messages1 = new List<string>();
            messages1.Add(CompletedTaskValidationMessage);
            messages1.Add(CompletedEvaluatorValidationMessage);
            ValidateMessageSuccessfullyLogged(messages1, "driver", DriverStdout, testFolder, 2);

            CleanUp(testFolder);
        }

        public IConfiguration DriverConfigurations()
        {
            return DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<ResubmitEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<ResubmitEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorCompleted, GenericType<ResubmitEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<ResubmitEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<ResubmitEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnTaskRunning, GenericType<ResubmitEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnContextFailed, GenericType<ResubmitEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<ResubmitEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnTaskFailed, GenericType<ResubmitEvaluatorDriver>.Class)
                .Build();
        }

        private sealed class ResubmitEvaluatorDriver : 
            IObserver<IDriverStarted>, 
            IObserver<IAllocatedEvaluator>,
            IObserver<ICompletedEvaluator>, 
            IObserver<IFailedEvaluator>, 
            IObserver<IRunningTask>,
            IObserver<IFailedContext>,
            IObserver<IFailedTask>, 
            IObserver<IActiveContext>, 
            IObserver<ICompletedTask>
        {
            private static readonly Logger Logger = Logger.GetLogger(typeof(ResubmitEvaluatorDriver));

            private readonly IEvaluatorRequestor _requestor;
            private int _taskNumber = 1;
            private int _contextNumber = 1;

            [Inject]
            private ResubmitEvaluatorDriver(IEvaluatorRequestor requestor)
            {
                _requestor = requestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().SetNumber(2).Build());
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                Logger.Log(Level.Info, "AllocatedEvaluator: " + value.Id);
                value.SubmitContext(
                    ContextConfiguration.ConfigurationModule
                        .Set(ContextConfiguration.Identifier, "ContextID" + _contextNumber++)
                        .Build());
            }

            public void OnNext(IActiveContext value)
            {
                Logger.Log(Level.Info, "ActiveContext: " + value.Id);
                value.SubmitTask(TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, TaskId + _taskNumber++)
                    .Set(TaskConfiguration.Task, GenericType<FailEvaluatorTask>.Class)
                    .Set(TaskConfiguration.OnMessage, GenericType<FailEvaluatorTask>.Class)
                    .Set(TaskConfiguration.OnClose, GenericType<FailEvaluatorTask>.Class)
                    .Build());
            }

            public void OnNext(IRunningTask value)
            {
                Logger.Log(Level.Info, "RunningTask: " + value.Id);
                string msg = value.Id.Equals(TaskId + "1") ? FailSignal : SuccSignal;
                value.Send(Encoding.UTF8.GetBytes(msg));
            }

            public void OnNext(ICompletedTask value)
            {
                Logger.Log(Level.Info, CompletedTaskValidationMessage + ". Task completed: " + value.Id);
                value.ActiveContext.Dispose();
            }

            public void OnNext(ICompletedEvaluator value)
            {
                Logger.Log(Level.Info, CompletedEvaluatorValidationMessage + ". Evaluator completed: " + value.Id);
            }

            public void OnNext(IFailedEvaluator value)
            {
                Logger.Log(Level.Info, FailedEvaluatorMessage + ". Evaluator failed: " + value.Id + "  FailedTaskId: " + value.FailedTask.Value.Id);
                foreach (var c in value.FailedContexts)
                {
                    Logger.Log(Level.Info, "Failed Context: " + c.Id);
                    Assert.Equal(c.Id, "ContextID1");
                }
                _requestor.Submit(_requestor.NewBuilder().Build());
            }

            public void OnNext(IFailedContext value)
            {
                Logger.Log(Level.Info, "In IFailedContext handler: " + value.Id);
            }

            public void OnNext(IFailedTask value)
            {
                Logger.Log(Level.Info, "In IFailedTask handler: " + value.Id);
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }
        }

        private sealed class FailEvaluatorTask : ITask, IDriverMessageHandler, IObserver<ICloseEvent>
        {
            private static readonly Logger Logger = Logger.GetLogger(typeof(FailEvaluatorTask));

            private readonly CountdownEvent _countdownEvent = new CountdownEvent(1);
            private string _message;

            [Inject]
            private FailEvaluatorTask()
            {
            }

            public byte[] Call(byte[] memento)
            {
                Logger.Log(Level.Info, "Hello in FailEvaluatorTask");
                _countdownEvent.Wait();

                if (_message.Equals(TestResubmitContext.FailSignal))
                {
                    Environment.Exit(1);
                }
                return null;
            }

            public void Handle(IDriverMessage value)
            {
                _message = ByteUtilities.ByteArraysToString(value.Message.Value);
                _countdownEvent.Signal();
            }

            public void OnNext(ICloseEvent value)
            {
                _countdownEvent.Signal();
            }

            public void Dispose()
            {
                throw new NotImplementedException();
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
}