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
    public sealed class TestCloseTask : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TestCloseTask));

        private const string DisposeMessageFromDriver = "DisposeMessageFromDriver";
        private const string NoMessage = "NO_MESSAGE";
        private const string CompletedValidationMessage = "CompletedValidationmessage";
        private const string FailToCloseTaskMessage = "FailToCloseTaskMessage";

        public TestCloseTask()
        {
            Init();
        }

        /// <summary>
        /// This test is run a normal task without close handler registered
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
            ////CleanUp(testFolder);
        }

        /// <summary>
        /// This test is to close a running task with exception throw during the closing
        /// </summary>
        [Fact]
        public void TestStopTaskWithExceptionOnLocalRuntime()
        {
            const string successIndication = "EXIT: ActiveContextClr2Java::Close";
            const string failedTaskIndication = "Java_org_apache_reef_javabridge_NativeInterop_clrSystemFailedTaskHandlerOnNext";

            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfigurations(DisposeMessageFromDriver, GetTaskConfigurationForFailtToCloseTask()), typeof(CloseTaskTestDriver), 1, "testStopTask", "local", testFolder);
            var messages = new List<string>();
            messages.Add(successIndication);
            ValidateMessageSuccessfullyLogged(messages, "driver", DriverStdout, testFolder, 1);

            var messages1 = new List<string>();
            messages.Add(DisposeMessageFromDriver);
            messages.Add(failedTaskIndication);
            ValidateMessageSuccessfullyLogged(messages1, "Node-*", EvaluatorStdout, testFolder, 2);
            ////CleanUp(testFolder);
        }

        /// <summary>
        /// This test is to close a running task over the bridge
        /// </summary>
        [Fact]
        public void TestTaskWithNoClosehandlerOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfigurations(DisposeMessageFromDriver, GetTaskConfigurationForNoCloseHandlerTask()), typeof(CloseTaskTestDriver), 1, "testStopTask", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, testFolder: testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(CompletedValidationMessage, testFolder, 1);
            ////CleanUp(testFolder);
        }

        /// <summary>
        /// This test is to close a running task over the bridge
        /// </summary>
        [Fact]
        public void TestStopTaskOnLocalRuntimeWithNullMessage()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfigurations(NoMessage, GetTaskConfigurationForCloseTask()), typeof(CloseTaskTestDriver), 1, "testStopTask", "local", testFolder);
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
                .Set(TaskConfiguration.Task, GenericType<TestCloseTask.CloseTestTask>.Class)
                .Set(TaskConfiguration.OnClose, GenericType<TestCloseTask.CloseTestTask>.Class)
                .Build();
        }

        private IConfiguration GetTaskConfigurationForFailtToCloseTask()
        {
            return TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "TaskID")
                .Set(TaskConfiguration.Task, GenericType<TestCloseTask.FailToCloseTask>.Class)
                .Set(TaskConfiguration.OnClose, GenericType<TestCloseTask.FailToCloseTask>.Class)
                .Build();
        }

        private IConfiguration GetTaskConfigurationForNoCloseHandlerTask()
        {
            return TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "TaskID")
                .Set(TaskConfiguration.Task, GenericType<TestCloseTask.NoCloseHandlerTask>.Class)
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
            private string _disposeMessage;
            private IConfiguration _taskConfiguration;

            [Inject]
            private CloseTaskTestDriver(IEvaluatorRequestor evaluatorRequestor,
                [Parameter(typeof(DisposeMessage))] string disposeMessage,
                [Parameter(typeof(TaskConfigurationString))] string taskConfigString,
                AvroConfigurationSerializer avroConfigurationSerializer)
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
                Assert.Contains(FailToCloseTaskMessage, failedExeption);
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

        private sealed class CloseTestTask : ITask, IObserver<ICloseEvent>
        {
            private readonly CountdownEvent _suspendSignal = new CountdownEvent(1);

            [Inject]
            private CloseTestTask()
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

        private sealed class FailToCloseTask : ITask, IObserver<ICloseEvent>
        {
            private readonly CountdownEvent _suspendSignal = new CountdownEvent(1);

            [Inject]
            private FailToCloseTask()
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

        private sealed class NoCloseHandlerTask : ITask
        {
            [Inject]
            private NoCloseHandlerTask()
            {
            }

            public byte[] Call(byte[] memento)
            {
                Logger.Log(Level.Info, "Hello in NoCloseHandlerTask");
                return null;
            }

            public void Dispose()
            {
                Logger.Log(Level.Info, "Task is disposed.");
            }
        }
    }
}