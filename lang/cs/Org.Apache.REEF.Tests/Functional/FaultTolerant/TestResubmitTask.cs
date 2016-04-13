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
    /// This is scenario testing. It is to test the following scenario to make sure the events, messages we receive are what expected.
    /// * Submit a task on an active context
    /// * After the task is running, driver sends an event to evaluator to close the task
    /// * Task throws exception with a message telling the driver that the task fails as instructed by driver
    /// * Driver receives the FailedTask event and resubmit a task on the existing context
    /// * In task IDriverMessage, verify the message send from drive is the same as what is expected
    /// * In task ICloseEvent, verify the message in the close event is the same as what is expected.
    /// The test can submit two evaluators/Contexts/Tasks and let both to close, and verify:
    /// * In IFailedTask, the task and context mappings are the same as the assignment before the task was submitted. 
    /// * In IFailedTask, the exception message in IFailedTask is the same as the one thrown in the Task 
    /// * In ICompletedTask, verify the task and context mapping are still remain the same as the assignment before the task was submitted. 
    /// Test Verification:
    /// * numberOfContextsToClose == 2
    /// * numberOfTasksToFail == 2
    /// * numberOfEvaluatorsToFail == 0
    /// If any of above verification fails, the test fails. 
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
        /// This test submits two evaluators/contexts/tasks, then close the two running tasks and resubmit two new tasks 
        /// on the existing active contexts. It is to verify events and messages received are the same as what we expected. 
        /// It is to verify we can submit tasks on existing contexts if previous tasks fail.
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

        /// <summary>
        /// Test driver
        /// </summary>
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

            /// <summary>
            /// Verify when exception is shown in task, IFailedTask will be received here with the message set in the task
            /// And verify the context associated with the failed task is the same as the context that the task was submitted
            /// </summary>
            /// <param name="value"></param>
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

            /// <summary>
            /// Close the first two tasks and send message to the 3rd and 4th tasks
            /// </summary>
            /// <param name="value"></param>
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

            /// <summary>
            /// When receiving closed task event, verify the command from the driver. If it matches expected message, 
            /// throw exception to close the task. Otherwise, signal the task to return, that would result in test failure 
            /// as the test expect two failed tasks. 
            /// </summary>
            /// <param name="value"></param>
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

            /// <summary>
            /// Expect the message from driver. If the message is the same as what is sent from driver, signal the task to properly return
            /// Otherwise, throw exception which would cause an unexpected failed task therefore failed test verification. 
            /// </summary>
            /// <param name="value"></param>
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