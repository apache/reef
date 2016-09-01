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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    /// <summary>
    /// This is to test task exceptions for IMRU tasks
    /// </summary>
    [Collection("FunctionalTests")]
    public class TestTaskExceptions : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TestTaskExceptions));
        private const string TaskId = "taskId";
        private const string ValidFailedTaskMessage = "ValidFailedTaskMessage";
        private const string TaskCompletedMessage = "TaskCompletedMessage";
        private const string InnerExceptionMessage = "InnerExceptionMessage";

        /// <summary>
        /// Test IMRUTaskAppException
        /// </summary>
        [Fact]
        public void TestTaskAppException()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfigurations(GetTaskConfiguration(TaskId + 1, TaskManager.TaskAppError)), typeof(FailedTaskDriver), 1, "TestTaskExceptions", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, 1, 0, testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(ValidFailedTaskMessage, testFolder, 1);
            ValidateMessageSuccessfullyLoggedForDriver(TaskCompletedMessage, testFolder, 0);
            CleanUp(testFolder);
        }

        /// <summary>
        /// Test IMRU TestTaskGroupCommunicationException
        /// </summary>
        [Fact]
        public void TestTaskGroupCommunicationException()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfigurations(GetTaskConfiguration(TaskId + 2, TaskManager.TaskGroupCommunicationError)), typeof(FailedTaskDriver), 1, "TestTaskExceptions", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, 1, 0, testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(ValidFailedTaskMessage, testFolder, 1);
            ValidateMessageSuccessfullyLoggedForDriver(TaskCompletedMessage, testFolder, 0);
            CleanUp(testFolder);
        }

        /// <summary>
        /// Test IMRUTaskSystemException
        /// </summary>
        [Fact]
        public void TestTaskSystemException()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfigurations(GetTaskConfiguration(TaskId + 3, TaskManager.TaskSystemError)), typeof(FailedTaskDriver), 1, "TestTaskExceptions", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, 1, 0, testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(ValidFailedTaskMessage, testFolder, 1);
            ValidateMessageSuccessfullyLoggedForDriver(TaskCompletedMessage, testFolder, 0);
            CleanUp(testFolder);
        }

        private IConfiguration DriverConfigurations(IConfiguration taskConfig)
        {
            var driverConfig = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<FailedTaskDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<FailedTaskDriver>.Class)
                .Set(DriverConfiguration.OnTaskFailed, GenericType<FailedTaskDriver>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<FailedTaskDriver>.Class)
                .Build();

            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
            return TangFactory.GetTang().NewConfigurationBuilder(driverConfig)
                .BindStringNamedParam<TaskConfigurationString>(serializer.ToString(taskConfig))
                .Build();
        }

        private IConfiguration GetTaskConfiguration(string taskId, string message)
        {
            var taskConfig = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, taskId)
                .Set(TaskConfiguration.Task, GenericType<ExceptionTask>.Class)
                .Build();

            var additionalConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<TaskExceptionMessage, string>(
                    GenericType<TaskExceptionMessage>.Class, message)
                .Build();

            return Configurations.Merge(additionalConfig, taskConfig);
        }

        private sealed class FailedTaskDriver : IObserver<IDriverStarted>, IObserver<IAllocatedEvaluator>,
            IObserver<IFailedTask>, IObserver<ICompletedTask>
        {
            private readonly IEvaluatorRequestor _requestor;
            private readonly IConfiguration _taskConfiguration;

            [Inject]
            private FailedTaskDriver(
                IEvaluatorRequestor requestor,
                [Parameter(typeof(TaskConfigurationString))] string taskConfigString,
                AvroConfigurationSerializer avroConfigurationSerializer)
            {
                _requestor = requestor;
                _taskConfiguration = avroConfigurationSerializer.FromString(taskConfigString);
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().Build());
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                value.SubmitTask(_taskConfiguration);
            }

            /// <summary>
            /// Verify Exception message and exception type for different task id. 
            /// </summary>
            /// <param name="value"></param>
            public void OnNext(IFailedTask value)
            {
                var msg = string.Format(CultureInfo.InvariantCulture,
                    "In IFailedTask, taskId: {0}, Message {1}, Exception {2}.",
                    value.Id,
                    value.Message,
                    value.AsError().GetType());
                Logger.Log(Level.Info, msg);

                if (value.Id.Equals(TaskId + 1))
                {
                    Assert.Equal(TaskManager.TaskAppError, value.Message);
                    if (value.AsError() == null || !(value.AsError() is IMRUTaskAppException))
                    {
                        throw new Exception(string.Format(CultureInfo.InvariantCulture, "Exception {0} should have been serialized properly.", typeof(IMRUTaskAppException)));
                    }
                }

                if (value.Id.Equals(TaskId + 2))
                {
                    Assert.Equal(TaskManager.TaskGroupCommunicationError, value.Message);

                    if (value.AsError() == null || !(value.AsError() is IMRUTaskGroupCommunicationException))
                    {
                        throw new Exception(string.Format(CultureInfo.InvariantCulture, "Exception {0} should have been serialized properly.", typeof(IMRUTaskGroupCommunicationException)));
                    }
                }

                if (value.Id.Equals(TaskId + 3))
                {
                    Assert.Equal(TaskManager.TaskSystemError, value.Message);
                    if (value.AsError() == null || !(value.AsError() is IMRUTaskSystemException))
                    {
                        throw new Exception(string.Format(CultureInfo.InvariantCulture, "Exception {0} should have been serialized properly.", typeof(IMRUTaskSystemException)));
                    }

                    Assert.Equal(InnerExceptionMessage, value.AsError().InnerException.Message);
                    Assert.True(value.AsError().InnerException is Exception);
                }
                Logger.Log(Level.Info, ValidFailedTaskMessage);
                value.GetActiveContext().Value.Dispose();
            }

            public void OnNext(ICompletedTask value)
            {
                Logger.Log(Level.Info, TaskCompletedMessage);
                throw new Exception("Did not expect a completed task.");
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

        /// <summary>
        /// Test task that throws exception based on the message specified in the constructor
        /// </summary>
        private sealed class ExceptionTask : ITask
        {
            private readonly string _taskExceptionMessage;

            [Inject]
            private ExceptionTask([Parameter(typeof(TaskExceptionMessage))] string taskExceptionMessage)
            {
                _taskExceptionMessage = taskExceptionMessage;
            }

            public void Dispose()
            {
            }

            /// <summary>
            /// Throws corresponding exception based on the message received. 
            /// </summary>
            /// <param name="memento"></param>
            /// <returns></returns>
            public byte[] Call(byte[] memento)
            {
                Logger.Log(Level.Info, "In ExceptionTask.Call(), _taskExceptionMessage: " + _taskExceptionMessage);
                switch (_taskExceptionMessage)
                {
                    case TaskManager.TaskAppError:
                        throw new IMRUTaskAppException(_taskExceptionMessage);
                    case TaskManager.TaskGroupCommunicationError:
                        throw new IMRUTaskGroupCommunicationException(_taskExceptionMessage);
                    default:
                        throw new IMRUTaskSystemException(_taskExceptionMessage, new Exception(InnerExceptionMessage));
                }
            }
        }

        [NamedParameter("Task exception message", "TaskExceptionMessage")]
        private class TaskExceptionMessage : Name<string>
        {
        }

        [NamedParameter("Task Configuration string", "TaskConfigurationString")]
        private class TaskConfigurationString : Name<string>
        {
        }
    }
}