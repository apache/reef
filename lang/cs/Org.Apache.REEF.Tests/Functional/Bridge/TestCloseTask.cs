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
using Org.Apache.REEF.Common.Tasks.Defaults;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Bridge
{
    /// <summary>
    /// This test is to close a running task from driver
    /// </summary>
    [Collection("FunctionalTests")]
    public sealed class TestCloseTask : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TestCloseTask));

        private const string DisposeMessageFromDriver = "DisposeMessageFromDriver";
        private const string NoMessage = "NO_MESSAGE";
        private const string CompletedValidationMessage = "CompletedValidationmessage";
        private const string FailToCloseTaskMessage = "FailToCloseTaskMessage";
        private const string BreakTaskMessage = "BreakTaskMessage";
        private const string EnforceToCloseMessage = "EnforceToCloseMessage";

        /// <summary>
        /// This test is close a running task with a close handler registered
        /// </summary>
        [Fact]
        public void TestStopTaskOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfigurations(DisposeMessageFromDriver, GetTaskConfigurationForCloseTask()), typeof(CloseTaskTestDriver), 1, "testStopTask", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, testFolder: testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(CompletedValidationMessage, testFolder, 1);
            var messages = new List<string>();
            messages.Add(DisposeMessageFromDriver);
            ValidateMessageSuccessfullyLogged(messages, "Node-*", EvaluatorStdout, testFolder, 2);
            CleanUp(testFolder);
        }

        /// <summary>
        /// This test is to close a running task and enforce it to break and return after the current iteration
        /// </summary>
        [Fact]
        public void TestBreakTaskOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfigurations(DisposeMessageFromDriver, GetTaskConfigurationForBreakTask()), typeof(CloseTaskTestDriver), 1, "TestBreakTask", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, testFolder: testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(CompletedValidationMessage, testFolder, 1);
            var messages = new List<string>();
            messages.Add(DisposeMessageFromDriver);
            messages.Add(BreakTaskMessage);
            ValidateMessageSuccessfullyLogged(messages, "Node-*", EvaluatorStdout, testFolder, -1);
            CleanUp(testFolder);
        }

        /// <summary>
        /// This test is to close a running task and enforce it to break and return after the current iteration
        /// </summary>
        [Fact]
        public void TestEnforceCloseTaskOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfigurations(DisposeMessageFromDriver, GetTaskConfigurationForEnforceToCloseTask()), typeof(CloseTaskTestDriver), 1, "TestEnforceCloseTask", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, 1, 0, testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(CompletedValidationMessage, testFolder, 0);
            var messages = new List<string>();
            messages.Add(DisposeMessageFromDriver);
            messages.Add(EnforceToCloseMessage);
            ValidateMessageSuccessfullyLogged(messages, "Node-*", EvaluatorStdout, testFolder, -1);
            CleanUp(testFolder);
        }

        /// <summary>
        /// This test is to close a running task with exception throw in close handler
        /// Expect to receive Exception in Failed Task event handler in driver
        /// </summary>
        [Fact]
        public void TestStopTaskWithExceptionOnLocalRuntime()
        {
            const string successIndication = "EXIT: ActiveContextClr2Java::Close";
            const string failedTaskIndication = "Java_org_apache_reef_javabridge_NativeInterop_clrSystemFailedTaskHandlerOnNext";

            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfigurations(DisposeMessageFromDriver, GetTaskConfigurationForFailToCloseTask()), typeof(CloseTaskTestDriver), 1, "testStopTaskWithException", "local", testFolder);
            var messages = new List<string>();
            messages.Add(successIndication);
            messages.Add(failedTaskIndication);
            ValidateMessageSuccessfullyLogged(messages, "driver", DriverStdout, testFolder, 1);

            var messages1 = new List<string>();
            messages1.Add(DisposeMessageFromDriver);
            ValidateMessageSuccessfullyLogged(messages1, "Node-*", EvaluatorStdout, testFolder, 2);
            CleanUp(testFolder);
        }

        /// <summary>
        /// This test is to close a running task over the bridge without close handler bound
        /// Expect to get TaskCloseHandlerNotBoundException
        /// </summary>
        [Fact]
        public void TestTaskWithNoCloseHandlerOnLocalRuntime()
        {
            const string closeHandlerNoBound = "ExceptionCaught TaskCloseHandlerNotBoundException";

            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfigurations(DisposeMessageFromDriver, GetTaskConfigurationForNoCloseHandlerTask()), typeof(CloseTaskTestDriver), 1, "testStopTaskWithNoCloseHandler", "local", testFolder);
            var messages = new List<string>();
            messages.Add(closeHandlerNoBound);
            ValidateMessageSuccessfullyLogged(messages, "Node-*", EvaluatorStdout, testFolder, 1);
            CleanUp(testFolder);
        }

        /// <summary>
        /// This test is to close a running task over the bridge with null message
        /// </summary>
        [Fact]
        public void TestStopTaskOnLocalRuntimeWithNullMessage()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfigurations(NoMessage, GetTaskConfigurationForCloseTask()), typeof(CloseTaskTestDriver), 1, "testStopTaskWithNullMessage", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, testFolder: testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(CompletedValidationMessage, testFolder, 1);
            var messages = new List<string>();
            messages.Add("Control protobuf to stop task");
            ValidateMessageSuccessfullyLogged(messages, "Node-*", EvaluatorStdout, testFolder, 1);
            CleanUp(testFolder);
        }

        private IConfiguration GetTaskConfigurationForCloseTask()
        {
            return TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "TaskID")
                .Set(TaskConfiguration.Task, GenericType<TestCloseTask.CloseByReturnTestTask>.Class)
                .Set(TaskConfiguration.OnClose, GenericType<TestCloseTask.CloseByReturnTestTask>.Class)
                .Build();
        }

        private IConfiguration GetTaskConfigurationForBreakTask()
        {
            return TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "TaskID")
                .Set(TaskConfiguration.Task, GenericType<TestCloseTask.CloseByBreakAndEnforceToStopTask>.Class)
                .Set(TaskConfiguration.OnClose, GenericType<TestCloseTask.CloseByBreakAndEnforceToStopTask>.Class)
                .Build();
        }
        private IConfiguration GetTaskConfigurationForEnforceToCloseTask()
        {
            var taskConfig = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "TaskID-EnforceToClose")
                .Set(TaskConfiguration.Task, GenericType<TestCloseTask.CloseByBreakAndEnforceToStopTask>.Class)
                .Set(TaskConfiguration.OnClose, GenericType<TestCloseTask.CloseByBreakAndEnforceToStopTask>.Class)
                .Build();

            return TangFactory.GetTang()
                .NewConfigurationBuilder(taskConfig)
                .BindIntNamedParam<EnforceCloseTimeoutMilliseconds>("1000")
                .BindNamedParameter<EnforceClose, bool>(GenericType<EnforceClose>.Class, "true")
                .Build();
        }

        private IConfiguration GetTaskConfigurationForFailToCloseTask()
        {
            return TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "TaskID-FailToClose")
                .Set(TaskConfiguration.Task, GenericType<TestCloseTask.CloseByThrowExceptionTask>.Class)
                .Set(TaskConfiguration.OnClose, GenericType<TestCloseTask.CloseByThrowExceptionTask>.Class)
                .Build();
        }

        private IConfiguration GetTaskConfigurationForNoCloseHandlerTask()
        {
            return TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "TaskID-NoCloseHandler")
                .Set(TaskConfiguration.Task, GenericType<TestCloseTask.MissingCloseHandlerTask>.Class)
                .Build();
        }

        /// <summary>
        /// Driver configuration for the test driver
        /// </summary>
        /// <returns></returns>
        public IConfiguration DriverConfigurations(string taskCloseMessage, IConfiguration taskConfig)
        {
            var handlerConfig = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<CloseTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<CloseTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<CloseTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnTaskRunning, GenericType<CloseTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<CloseTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnTaskFailed, GenericType<CloseTaskTestDriver>.Class)
                .Build();

            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
            var messageConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindStringNamedParam<DisposeMessage>(taskCloseMessage)
                .BindStringNamedParam<TaskConfigurationString>(serializer.ToString(taskConfig))
                .Build();

            return Configurations.Merge(handlerConfig, messageConfig);
        }

        [NamedParameter("Message send with task close", "TaskDisposeMessage", NoMessage)]
        private class DisposeMessage : Name<string> 
        {
        }

        [NamedParameter("Task Configuration string", "TaskConfigurationString")]
        private class TaskConfigurationString : Name<string>
        {
        }

        private sealed class CloseTaskTestDriver :
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<IActiveContext>,
            IObserver<ICompletedTask>,
            IObserver<IFailedTask>,
            IObserver<IRunningTask>           
        {
            private readonly IEvaluatorRequestor _requestor;
            private int _contextNumber = 0;
            private readonly string _disposeMessage;
            private readonly IConfiguration _taskConfiguration;

            [Inject]
            private CloseTaskTestDriver(IEvaluatorRequestor evaluatorRequestor,
                [Parameter(typeof(DisposeMessage))] string disposeMessage,
                [Parameter(typeof(TaskConfigurationString))] string taskConfigString,
                IConfigurationSerializer avroConfigurationSerializer)
            {
                _requestor = evaluatorRequestor;
                _disposeMessage = disposeMessage;
                _taskConfiguration = avroConfigurationSerializer.FromString(taskConfigString);
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().SetNumber(1).Build());
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                value.SubmitContext(
                    ContextConfiguration.ConfigurationModule
                        .Set(ContextConfiguration.Identifier, "ContextID" + _contextNumber++)
                        .Build());
            }

            public void OnNext(IActiveContext value)
            {
                value.SubmitTask(_taskConfiguration);
            }

            public void OnNext(ICompletedTask value)
            {
                // Log on task completion to signal a passed test.
                Logger.Log(Level.Info, CompletedValidationMessage + ". Task completed: " + value.Id);
                value.ActiveContext.Dispose();
            }

            public void OnNext(IFailedTask value)
            {
                var failedExeption = ByteUtilities.ByteArraysToString(value.Data.Value);
                Logger.Log(Level.Error, "In IFailedTask: " + failedExeption);

                if (value.Id.EndsWith("TaskID-FailToClose"))
                {
                    Assert.Contains(FailToCloseTaskMessage, failedExeption);
                }
                if (value.Id.EndsWith("TaskID-NoCloseHandler"))
                {
                    Assert.Contains(DefaultTaskCloseHandler.ExceptionMessage, failedExeption);
                }
                if (value.Id.EndsWith("TaskID-EnforceToClose"))
                {
                    Assert.Contains(TaskManager.TaskKilledByDriver, failedExeption);
                }

                value.GetActiveContext().Value.Dispose();
            }

            public void OnNext(IRunningTask value)
            {
                Logger.Log(Level.Info, "Task running: " + value.Id);
                if (_disposeMessage.Equals(NoMessage))
                {
                    value.Dispose();
                }
                else
                {
                    value.Dispose(Encoding.UTF8.GetBytes(_disposeMessage));
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

        /// <summary>
        /// This test task receives close event, then signals Call() method to properly return.
        /// </summary>
        private sealed class CloseByReturnTestTask : ITask, IObserver<ICloseEvent>
        {
            private readonly CountdownEvent _suspendSignal = new CountdownEvent(1);

            [Inject]
            private CloseByReturnTestTask([Parameter(typeof(EnforceCloseTimeoutMilliseconds))] int enforceCloseTimeout)
            {
            }

            public byte[] Call(byte[] memento)
            {
                Logger.Log(Level.Info, "Hello in StopTestTask");
                _suspendSignal.Wait();
                return null;
            }

            public void Dispose()
            {
                Logger.Log(Level.Info, "Task is disposed.");
            }

            public void OnNext(ICloseEvent value)
            {
                try
                {
                    if (value.Value != null && value.Value.Value != null)
                    {
                        Logger.Log(Level.Info, "Closed event received in task:" + Encoding.UTF8.GetString(value.Value.Value));
                        Assert.Equal(Encoding.UTF8.GetString(value.Value.Value), DisposeMessageFromDriver);
                    }                    
                }
                finally
                {
                    _suspendSignal.Signal();
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

        /// <summary>
        /// This is a testing task. It serves for two test cases.
        /// In the first case, EnforceClose is false (default). When the task receives the close event, it signals the Call method
        /// to let it continue the iteration. As _shouldCloseTask is set to 1, the Call() will return after
        /// completing the current iteration.
        /// In the second case, EnforceClose is set to true. When the task receives the close event, it sets
        /// _shouldCloseTask to 1. As the task is hung in this scenario, Call() would never return.
        ///  After waiting for _enforceCloseTimeoutMilliseconds, the close handler throws an exception, enforcing the task to stop.
        /// </summary>
        private sealed class CloseByBreakAndEnforceToStopTask : ITask, IObserver<ICloseEvent>
        {
            private long _shouldCloseTask = 0;
            private long _isTaskStopped = 0;
            private readonly bool _enforceClose;
            private readonly int _enforceCloseTimeoutMilliseconds;

            private readonly CountdownEvent _suspendSignal1 = new CountdownEvent(1);
            private readonly CountdownEvent _suspendSignal2 = new CountdownEvent(1);
            private readonly ManualResetEventSlim _waitToCloseEvent = new ManualResetEventSlim(false);

            [Inject]
            private CloseByBreakAndEnforceToStopTask(
                [Parameter(typeof(EnforceCloseTimeoutMilliseconds))] int enforceCloseTimeoutMilliseconds,
                [Parameter(typeof(EnforceClose))] bool enforceClose)
            {
                _enforceClose = enforceClose;
                _enforceCloseTimeoutMilliseconds = enforceCloseTimeoutMilliseconds;
            }

            public byte[] Call(byte[] memento)
            {
                int iterate = 1;

                while (Interlocked.Read(ref _shouldCloseTask) == 0 && iterate < 100)
                {
                    iterate++;
                    if (_enforceClose)
                    {
                        _suspendSignal1.Wait();
                    }
                    else
                    {
                        _suspendSignal2.Wait();
                    }
                }

                Interlocked.Exchange(ref _isTaskStopped, 1);

                if (Interlocked.Read(ref _shouldCloseTask) == 1)
                {
                    Logger.Log(Level.Info, BreakTaskMessage);
                    _waitToCloseEvent.Set();
                }

                return null;
            }

            public void Dispose()
            {
                Logger.Log(Level.Info, "Task is disposed.");
            }

            /// <summary>
            /// When the close event is received, it sets _shouldCloseTask to 1.
            /// If _enforceClose is false, _suspendSignal2 is signaled to let the task to continue to run. This is to simulate that the
            /// task is running properly and will break after completing the current iteration. It will set the _waitToCloseEvent
            /// to let the flow in the close event handler to continue.
            /// If _enforceClose is true,  _suspendSignal1 will be not signaled, this is to simulate that the task is hung.
            /// After waiting for specified time, the close handler will throw exception to enforce the task to stop.
            /// </summary>
            /// <param name="closeEvent"></param>
            public void OnNext(ICloseEvent closeEvent)
            {
                if (closeEvent.Value.IsPresent() && Encoding.UTF8.GetString(closeEvent.Value.Value).Equals(DisposeMessageFromDriver))
                {
                    Logger.Log(Level.Info, "Closed event received in task:" + Encoding.UTF8.GetString(closeEvent.Value.Value));
                    Interlocked.Exchange(ref _shouldCloseTask, 1);
                    if (!_enforceClose)
                    {
                        _suspendSignal2.Signal();
                    }

                    _waitToCloseEvent.Wait(TimeSpan.FromMilliseconds(_enforceCloseTimeoutMilliseconds));

                    if (Interlocked.Read(ref _isTaskStopped) == 0)
                    {
                        Logger.Log(Level.Info, EnforceToCloseMessage);
                        throw new IMRUTaskSystemException(TaskManager.TaskKilledByDriver);
                    }
                }
                else
                {
                    throw new Exception("Expected close event message is not received.");
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

        /// <summary>
        /// This is a test task for the scenario in which the task receives close event, instead of
        /// let the task to return properly, it throws exception.
        /// </summary>
        private sealed class CloseByThrowExceptionTask : ITask, IObserver<ICloseEvent>
        {
            private readonly CountdownEvent _suspendSignal = new CountdownEvent(1);

            [Inject]
            private CloseByThrowExceptionTask()
            {
            }

            public byte[] Call(byte[] memento)
            {
                Logger.Log(Level.Info, "Hello in FailtToCloseTask");
                _suspendSignal.Wait();
                return null;
            }

            public void Dispose()
            {
                Logger.Log(Level.Info, "Task is disposed.");
            }

            public void OnNext(ICloseEvent value)
            {
                try
                {
                    if (value.Value != null && value.Value.Value != null)
                    {
                        Logger.Log(Level.Info, "Closed event received in task:" + Encoding.UTF8.GetString(value.Value.Value));
                        Assert.Equal(Encoding.UTF8.GetString(value.Value.Value), DisposeMessageFromDriver);
                    }
                }
                finally
                {
                    throw new Exception(FailToCloseTaskMessage);
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

        /// <summary>
        /// This task doesn't implement close handler. It is to test closeHandlerNoBound exception.
        /// </summary>
        private sealed class MissingCloseHandlerTask : ITask
        {
            [Inject]
            private MissingCloseHandlerTask()
            {
            }

            public byte[] Call(byte[] memento)
            {
                Logger.Log(Level.Info, "Hello in NoCloseHandlerTask");
                Thread.Sleep(50 * 1000);
                return null;
            }

            public void Dispose()
            {
                Logger.Log(Level.Info, "Task is disposed.");
            }
        }

        [NamedParameter("Enforce the task to close", "EnforceClose", "false")]
        private sealed class EnforceClose : Name<bool>
        {
        }
    }
}