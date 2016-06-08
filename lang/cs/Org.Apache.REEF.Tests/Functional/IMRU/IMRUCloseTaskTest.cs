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
        /// It first informs the Call method to stop.
        /// If Call method is running properly, it will respect to this flag and will return properly, that will end up ICompletedTask event.
        ////If Call method is hung some where and cannot be returned, the close handler will throw exception, that would cause IFailedTask event.
        /// As we are testing IMRU Task not a test task, the behavior is not deterministic. It can be CompletedTask or FailedTask
        /// No matter how the task is closed, the total number of completed task and failed task should be equal to the 
        /// total number of the tasks.
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
            string[] lines = ReadLogFile(DriverStdout, "driver", testFolder);
            int failedCount = GetMessageCount(lines, FailTaskMessage);
            int completedCount = GetMessageCount(lines, CompletedTaskMessage);
            Assert.Equal(numTasks, failedCount + completedCount);
            CleanUp(testFolder);
        }

        /// <summary>
        /// Same testing for running on YARN
        /// It sends close event for all the running tasks.
        /// It first informs the Call method to stop.
        /// If Call method is running properly, it will respect to this flag and will return properly, that will end up ICompletedTask event.
        ////If Call method is hung some where and cannot be returned, the close handler will throw exception, that would cause IFailedTask event.
        /// As we are testing IMRU Task not a test task, the behavior is not deterministic. It can be CompletedTask or FailedTask
        /// No matter how the task is closed, the total number of completed task and failed task should be equal to the 
        /// total number of the tasks.
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
                    GenericType<TestHandlers>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnEvaluatorFailed,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnContextFailed,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskFailed,
                    GenericType<TestHandlers>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskRunning,
                    GenericType<TestHandlers>.Class)
                .Set(REEF.Driver.DriverConfiguration.CustomTraceLevel, TraceLevel.Info.ToString())
                .Build();
        }

        /// <summary>
        /// Test handlers
        /// </summary>
        internal sealed class TestHandlers : IObserver<IRunningTask>, IObserver<IFailedTask>, IObserver<ICompletedTask>
        {
            [Inject]
            private TestHandlers()
            {
            }

            /// <summary>
            /// Log the task id and dispose the context
            /// </summary>
            public void OnNext(IRunningTask value)
            {
                Logger.Log(Level.Info, "Received running task, closing it" + value.Id);
                value.Dispose(ByteUtilities.StringToByteArrays(TaskManager.CloseTaskByDriver));
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

            /// <summary>
            /// Log the task id and dispose the context
            /// </summary>
            public void OnNext(ICompletedTask value)
            {
                Logger.Log(Level.Info, CompletedTaskMessage + value.Id);
                value.ActiveContext.Dispose();
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