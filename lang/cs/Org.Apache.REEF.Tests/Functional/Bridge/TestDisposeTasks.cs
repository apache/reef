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
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Tests.Functional.Bridge.Exceptions;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Bridge
{
    [Collection("FunctionalTests")]
    public class TestDisposeTasks : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TestDisposeTasks));

        private const string ExitByReturn = "ExitByReturn";
        private const string ExitByException = "ExitByException";
        private const string TaskIsDisposed = "TaskIsDisposed";
        private const string TaskKilledByDriver = "TaskKilledByDriver";
        private const string TaskId = "TaskId";
        private const string ContextId = "ContextId";

        /// <summary>
        /// Test scenario: Task returned properly then disposed
        /// </summary>
        [Fact]
        public void TestDisposeInTaskNormalReturnOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfigurations(1), typeof(TestDisposeTasks), 1, "TestDisposeTasks", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, 0, 0, testFolder);
            var messages = new List<string> { TaskIsDisposed };
            ValidateMessageSuccessfullyLogged(messages, "Node-*", EvaluatorStdout, testFolder, 1);
            ////CleanUp(testFolder);
        }

        /// <summary>
        /// Test scenario: Task is enforced to close after receiving close event
        /// </summary>
        [Fact]
        public void TestDisposeInTaskExceptionOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfigurations(2), typeof(TestDisposeTasks), 1, "TestDisposeTasks", "local", testFolder);
            ValidateSuccessForLocalRuntime(0, 0, 1, testFolder);
            var messages = new List<string> { TaskIsDisposed };
            ValidateMessageSuccessfullyLogged(messages, "Node-*", EvaluatorStdout, testFolder, 1);
            CleanUp(testFolder);
        }

        /// <summary>
        /// Test scenario: Dispose context while the task is still running.
        /// </summary>
        [Fact]
        public void TestDisposeFromContextInRunningOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfigurations(3), typeof(TestDisposeTasks), 1, "TestDisposeTasks", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, 0, 0, testFolder);
            var messages = new List<string>();
            messages.Add(TaskIsDisposed);
            ValidateMessageSuccessfullyLogged(messages, "Node-*", EvaluatorStdout, testFolder, 1);
            CleanUp(testFolder);
        }

        /// <summary>
        /// Driver configuration for the test driver
        /// </summary>
        /// <returns></returns>
        public IConfiguration DriverConfigurations(int taskNumber)
        {
            var taskIdConfig = TangFactory.GetTang()
                .NewConfigurationBuilder()
                .BindStringNamedParam<TaskNumber>(taskNumber.ToString())
                .Build();

            var driverConfig = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<DisposeTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<DisposeTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<DisposeTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnTaskRunning, GenericType<DisposeTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<DisposeTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<DisposeTaskTestDriver>.Class)
                .Build();

            return Configurations.Merge(taskIdConfig, driverConfig);
        }

        /// <summary>
        /// Test driver
        /// </summary>
        private sealed class DisposeTaskTestDriver :
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<IActiveContext>,
            IObserver<ICompletedTask>,
            IObserver<IFailedEvaluator>,
            IObserver<IRunningTask>            
        {
            private readonly IEvaluatorRequestor _requestor;            
            private readonly string _taskNumber;

            [Inject]
            private DisposeTaskTestDriver(IEvaluatorRequestor evaluatorRequestor,
                [Parameter(typeof(TaskNumber))] string taskNumber)
            {
                _requestor = evaluatorRequestor;
                _taskNumber = taskNumber;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().SetNumber(1).Build());
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                value.SubmitContext(
                    ContextConfiguration.ConfigurationModule
                        .Set(ContextConfiguration.Identifier, ContextId)
                        .Build());
            }

            public void OnNext(IActiveContext value)
            {
                value.SubmitTask(GetTaskConfigurationForCloseTask(TaskId + _taskNumber));
            }

            public void OnNext(ICompletedTask value)
            {
                Logger.Log(Level.Info, "Task completed: " + value.Id);
                Assert.Equal(TaskId + "1", value.Id);
                value.ActiveContext.Dispose();
            }

            /// <summary>
            /// Verify when exception is shown in task, IFailedTask will be received here with the message set in the task
            /// And verify the context associated with the failed task is the same as the context that the task was submitted
            /// </summary>
            /// <param name="value"></param>
            public void OnNext(IFailedEvaluator value)
            {
                Assert.True(value.FailedTask.IsPresent());
                Assert.Equal(TaskId + "2", value.FailedTask.Value.Id);

                var e = value.EvaluatorException.InnerException;
                Logger.Log(Level.Error, "In IFailedTask: e.type: {0}, e.message: {1}.", e.GetType(), e.Message);

                Assert.Equal(typeof(TestSerializableException), e.GetType());
                Assert.Equal(TaskKilledByDriver, e.Message);
            }

            /// <summary>
            /// Task1: Close task and expect it to return from Call()
            /// Task2: Close the task and expect it throw exception
            /// Task3: Let context Dispose to close a running task and make sure the task is disposed
            /// </summary>
            /// <param name="value"></param>
            public void OnNext(IRunningTask value)
            {
                Logger.Log(Level.Info, "Task running: " + value.Id);
                switch (value.Id)
                {
                    case TaskId + "1":
                        value.Dispose(Encoding.UTF8.GetBytes(ExitByReturn));
                        break;
                    case TaskId + "2":
                        value.Dispose(Encoding.UTF8.GetBytes(ExitByException));
                        break;
                    case TaskId + "3":
                        value.ActiveContext.Dispose();
                        break;
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
                    .Set(TaskConfiguration.Task, GenericType<CloseAndDisposeTask>.Class)
                    .Set(TaskConfiguration.OnClose, GenericType<CloseAndDisposeTask>.Class)
                    .Build();
            }
        }

        private sealed class CloseAndDisposeTask : ITask, IObserver<ICloseEvent>
        {
            private readonly CountdownEvent _suspendSignal = new CountdownEvent(1);
            private int _disposed = 0;

            [Inject]
            private CloseAndDisposeTask()
            {
            }

            public byte[] Call(byte[] memento)
            {
                Logger.Log(Level.Info, "Hello in CloseAndDisposeTask");
                _suspendSignal.Wait();
                return null;
            }

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) == 0)
                {
                    Logger.Log(Level.Info, TaskIsDisposed);
                }
            }

            /// <summary>
            /// Case 1: if message is ExitByReturn, signal the Call() to make the task return.  
            /// Case 2: if message is ExitByException, throw exception to expect the driver to receive FailedTask. 
            /// Otherwise do nothing, expecting TaskRuntime to dispose the task. 
            /// </summary>
            /// <param name="closeEvent"></param>
            public void OnNext(ICloseEvent closeEvent)
            {
                if (closeEvent.Value != null)
                {
                    if (closeEvent.Value.IsPresent())
                    {
                        string msg = Encoding.UTF8.GetString(closeEvent.Value.Value);
                        Logger.Log(Level.Info, "Closed event received in task:" + msg);

                        if (msg.Equals(ExitByReturn))
                        {
                            _suspendSignal.Signal();
                        }
                        else if (msg.Equals(ExitByException))
                        {
                            throw new TestSerializableException(TaskKilledByDriver);
                        }
                    }
                    else
                    {
                        Logger.Log(Level.Info, "Closed event received in task with no message");
                    }
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

        [NamedParameter]
        private class TaskNumber : Name<string>
        {            
        }        
    }
}
