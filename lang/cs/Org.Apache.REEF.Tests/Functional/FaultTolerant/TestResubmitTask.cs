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
    /// <summary>
    /// This test is to close a running task from driver
    /// </summary>
    [Collection("FunctionalTests")]
    public sealed class TestResubimitTask : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TestResubimitTask));

        private const string KillTaskCommandFromDriver = "KillTaskCommandFromDriver";
        private const string CompleteTaskCommandFromDriver = "CompleteTaskCommandFromDriver";
        private const string TaskKilledByDriver = "TaskKilledByDriver";
        private const string UnExpectedCloseMessage = "UnExpectedCloseMessage";
        private const string UnExpectedCompleteMessage = "UnExpectedCompleteMessage";

        /// <summary>
        /// This test is to close a running task and resubmit one on the existing context
        /// </summary>
        [Fact]
        public void TestStopAndResubmitTaskOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfigurations(), typeof(ResubmitTaskTestDriver), 2, "TestResubimitTask", "local", testFolder);
            ValidateSuccessForLocalRuntime(2, 2, 0, testFolder);
            CleanUp(testFolder);
        }

        /// <summary>
        /// Driver configuration for the test driver
        /// </summary>
        /// <returns></returns>
        public IConfiguration DriverConfigurations()
        {
            return DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<ResubmitTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<ResubmitTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<ResubmitTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnTaskRunning, GenericType<ResubmitTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<ResubmitTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnTaskFailed, GenericType<ResubmitTaskTestDriver>.Class)
                .Build();
        }

        private sealed class ResubmitTaskTestDriver :
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<IActiveContext>,
            IObserver<ICompletedTask>,
            IObserver<IFailedTask>,
            IObserver<IRunningTask>
        {
            private readonly IEvaluatorRequestor _requestor;
            private const string TaskId = "TaskId";
            private int _taskNumber = 1;
            private const string ContextId = "ContextId";
            private int _contextNumber = 1;
            private readonly IDictionary<string, string> _taskContextMapping = new Dictionary<string, string>();
            private readonly object _lock = new object();

            [Inject]
            private ResubmitTaskTestDriver(IEvaluatorRequestor evaluatorRequestor)
            {
                _requestor = evaluatorRequestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().SetNumber(2).Build());
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                lock (_lock)
                {
                    value.SubmitContext(
                        ContextConfiguration.ConfigurationModule
                            .Set(ContextConfiguration.Identifier, ContextId + _contextNumber)
                            .Build());
                    _contextNumber++;
                }
            }

            public void OnNext(IActiveContext value)
            {
                lock (_lock)
                {
                    value.SubmitTask(GetTaskConfigurationForCloseTask(TaskId + _taskNumber));
                    _taskContextMapping.Add(TaskId + _taskNumber, value.Id);
                    _taskNumber++;
                }
            }

            public void OnNext(ICompletedTask value)
            {
                Logger.Log(Level.Info, "Task completed: " + value.Id);
                VerifyContextTaskMapping(value.Id, value.ActiveContext.Id);
                value.ActiveContext.Dispose();
            }

            public void OnNext(IFailedTask value)
            {
                var failedExeption = ByteUtilities.ByteArraysToString(value.Data.Value);
                Logger.Log(Level.Error, "In IFailedTask: " + failedExeption);

                VerifyContextTaskMapping(value.Id, value.GetActiveContext().Value.Id);
                Assert.Contains(TaskKilledByDriver, failedExeption);

                OnNext(value.GetActiveContext().Value);
            }

            private void VerifyContextTaskMapping(string taskId, string contextId)
            {
                lock (_lock)
                {
                    string expectedContextId;
                    _taskContextMapping.TryGetValue(taskId, out expectedContextId);
                    Assert.Equal(expectedContextId, contextId);
                }
            }

            public void OnNext(IRunningTask value)
            {
                Logger.Log(Level.Info, "Task running: " + value.Id);
                switch (value.Id)
                {
                    case TaskId + "1":
                    case TaskId + "2":
                        value.Dispose(Encoding.UTF8.GetBytes(KillTaskCommandFromDriver));
                        break;
                    case TaskId + "3":
                    case TaskId + "4":
                        value.Send(Encoding.UTF8.GetBytes(CompleteTaskCommandFromDriver));
                        break;
                    default: 
                        throw new Exception("It should not be reached.");
                }
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            private static IConfiguration GetTaskConfigurationForCloseTask(string taskId)
            {
                return TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, taskId)
                    .Set(TaskConfiguration.Task, GenericType<ResubmitTask>.Class)
                    .Set(TaskConfiguration.OnClose, GenericType<ResubmitTask>.Class)
                    .Set(TaskConfiguration.OnMessage, GenericType<ResubmitTask>.Class)
                    .Build();
            }
        }

        private sealed class ResubmitTask : ITask, IDriverMessageHandler, IObserver<ICloseEvent>
        {
            private readonly CountdownEvent _suspendSignal = new CountdownEvent(1);

            [Inject]
            private ResubmitTask()
            {
            }

            public byte[] Call(byte[] memento)
            {
                Logger.Log(Level.Info, "Hello in ResubmitTask");
                _suspendSignal.Wait();
                return null;
            }

            public void Dispose()
            {
                Logger.Log(Level.Info, "Task is disposed.");
            }

            public void OnNext(ICloseEvent value)
            {
                if (value.Value != null && value.Value.Value != null)
                {
                    Logger.Log(Level.Info,
                        "Closed event received in task:" + Encoding.UTF8.GetString(value.Value.Value));
                    if (Encoding.UTF8.GetString(value.Value.Value).Equals(KillTaskCommandFromDriver))
                    {
                        throw new Exception(TaskKilledByDriver);
                    }
                    Logger.Log(Level.Error, UnExpectedCloseMessage);
                    _suspendSignal.Signal();
                }
            }

            public void Handle(IDriverMessage value)
            {
                var message = ByteUtilities.ByteArraysToString(value.Message.Value);
                Logger.Log(Level.Info, "Complete task message received in task:" + message);
                if (message.Equals(CompleteTaskCommandFromDriver))
                {
                    _suspendSignal.Signal();
                }
                else
                {
                    Logger.Log(Level.Error, UnExpectedCompleteMessage);
                    throw new Exception(UnExpectedCompleteMessage);                    
                }
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