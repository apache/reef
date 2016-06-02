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
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;
using TraceLevel = System.Diagnostics.TraceLevel;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    /// <summary>
    /// This is to test close event handler in IMRU tasks
    /// The test provide IRunningTask, IFailedTask and ICOmpletedTask handlers so that to trigger close events and handle the 
    /// failed tasks and completed tasks
    /// </summary>
    [Collection("FunctionalTests")]
    public class IMRUCloseTaskTest : IMRUBrodcastReduceTestBase
    {
        private const string CompletedTaskMessage = "CompletedTaskMessage";
        private const string FailTaskMessage = "FailTaskMessage";

        /// <summary>
        /// This test is for running in local runtime
        /// It sends close event for all the running tasks.
        /// For tasks that can close properly after receiving the close event, the driver will get ICompletedTask event. 
        /// For tasks that cannot close properly after receiving the close event, they will throw exceptions
        /// and the driver will get IFailedTask event. 
        /// </summary>
        [Fact]
        public void TestTaskCloseOnLocalRuntime()
        {
            int chunkSize = 2;
            int dims = 50;
            int iterations = 200;
            int mapperMemory = 5120;
            int updateTaskMemory = 5120;
            int numTasks = 4;
            string testFolder = DefaultRuntimeFolder + TestId;
            TestBroadCastAndReduce(false, numTasks, chunkSize, dims, iterations, mapperMemory, updateTaskMemory, testFolder);
            int failedCount = GetMessageCount(FailTaskMessage, "driver", DriverStdout, testFolder);
            int completedCount = GetMessageCount(CompletedTaskMessage, "driver", DriverStdout, testFolder);
            Assert.Equal(numTasks, failedCount + completedCount);
            CleanUp(testFolder);
        }

        /// <summary>
        /// Same testing for running on YARN
        /// It sends close event for all the running tasks.
        /// For tasks that can close properly after receiving the close event, the driver will get ICompletedTask event. 
        /// For those tasks that cannot close properly after receiving the close event, they will throw exceptions
        /// and the driver will get IFailedTask event. 
        /// </summary>
        [Fact(Skip = "Requires Yarn")]
        public void TestTaskCloseOnLocalRuntimeOnYarn()
        {
            int chunkSize = 2;
            int dims = 50;
            int iterations = 200;
            int mapperMemory = 5120;
            int updateTaskMemory = 5120;
            int numTasks = 4;
            TestBroadCastAndReduce(true, numTasks, chunkSize, dims, iterations, mapperMemory, updateTaskMemory);
        }

        /// <summary>
        /// This method overrides base class method and defines its own event handlers for driver. 
        /// It uses its own RunningTaskHandler, FailedTaskHandler and CompletedTaskHandler so that to simulate the test scenarios 
        /// and verify the test result. 
        /// Rest of the event handlers use those from IMRUDriver. In IActiveContext handler in IMRUDriver, IMRU tasks are bound for the test.
        /// </summary>
        /// <typeparam name="TMapInput"></typeparam>
        /// <typeparam name="TMapOutput"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <typeparam name="TPartitionType"></typeparam>
        /// <returns></returns>
        protected override IConfiguration DriverEventHandlerConfigurations<TMapInput, TMapOutput, TResult, TPartitionType>()
        {
            return REEF.Driver.DriverConfiguration.ConfigurationModule
                .Set(REEF.Driver.DriverConfiguration.OnEvaluatorAllocated,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnDriverStarted,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnContextActive,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskCompleted,
                    GenericType<CompletedTaskHandler>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnEvaluatorFailed,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnContextFailed,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskFailed,
                    GenericType<FailedTaskHandler>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskRunning,
                    GenericType<RunningTaskHandler>.Class)
                .Set(REEF.Driver.DriverConfiguration.CustomTraceLevel, TraceLevel.Info.ToString())
                .Build();
        }

        /// <summary>
        /// IRunningTask event handler. It sends close event to each running task
        /// </summary>
        internal sealed class RunningTaskHandler : IObserver<IRunningTask>
        {
            [Inject]
            private RunningTaskHandler()
            {                
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            /// <summary>
            /// Log the task id and dispose the context
            /// </summary>
            public void OnNext(IRunningTask value)
            {
                Logger.Log(Level.Info, "Received running task, closing it" + value.Id);
                value.Dispose(ByteUtilities.StringToByteArrays(TaskManager.CloseTaskByDriver));
            }
        }

        /// <summary>
        /// IFailedTask event handler. For tasks that throw exception after receiving close event, 
        /// we would expect to receive IFailedTask event in this handler.
        /// </summary>
        internal sealed class FailedTaskHandler : IObserver<IFailedTask>
        {
            [Inject]
            private FailedTaskHandler()
            {
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            /// <summary>
            /// Validate the event and dispose the context
            /// </summary>
            /// <param name="value"></param>
            public void OnNext(IFailedTask value)
            {
                Logger.Log(Level.Info, FailTaskMessage + value.Id);
                var failedExeption = ByteUtilities.ByteArraysToString(value.Data.Value);
                Assert.Contains(TaskManager.TaskKilledByDriver, failedExeption);
                value.GetActiveContext().Value.Dispose();
            }
        }

        /// <summary>
        /// ICompletedTask handler. For task that properly returns from Call() method after receiving close event
        /// we would expect to receive ICompletedTask event in this handler.
        /// </summary>
        internal sealed class CompletedTaskHandler : IObserver<ICompletedTask>
        {
            [Inject]
            private CompletedTaskHandler()
            {
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            /// <summary>
            /// Log the task id and dispose the context
            /// </summary>
            public void OnNext(ICompletedTask value)
            {
                Logger.Log(Level.Info, CompletedTaskMessage + value.Id);
                value.ActiveContext.Dispose();
            }
        }
    }
}