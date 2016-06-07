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
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.Network;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
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
    /// The test provide IRunningTask, IFailedTask and ICompletedTask handlers so that to trigger close events and handle the
    /// failed tasks and completed tasks
    /// </summary>
    [Collection("FunctionalTests")]
    public class IMRUCloseTaskTest : IMRUBrodcastReduceTestBase
    {
        /// <summary>
        /// This test is for running in local runtime
        /// It sends close event for all the running tasks.
        /// In the task close handler, the cancellation token will be set, and as a result tasks will return from the Call()
        /// method and driver will receive ICompletedTask.
        /// In the exceptional case, task might throw exception from Call() method, as a result, driver will receive IFailedTask.
        /// Expect number of CompletedTask and FailedTask equals to the total number of tasks. No failed Evaluator.
        /// </summary>
        [Fact]
        public void TestTaskCloseOnLocalRuntime()
        {
            const int chunkSize = 2;
            const int dims = 50;
            const int iterations = 200;
            const int mapperMemory = 512;
            const int updateTaskMemory = 512;
            const int numTasks = 4;
            const int numOfRetryInRecovery = 4;
            var testFolder = DefaultRuntimeFolder + TestId;
            TestBroadCastAndReduce(false, numTasks, chunkSize, dims, iterations, mapperMemory, updateTaskMemory, numOfRetryInRecovery, testFolder);
            string[] lines = ReadLogFile(DriverStdout, "driver", testFolder, 120);
            var completedCount = GetMessageCount(lines, CompletedTaskMessage);
            var failedCount = GetMessageCount(lines, FailedTaskMessage);
            Assert.Equal(numTasks, completedCount + failedCount);
            CleanUp(testFolder);
        }

        /// <summary>
        /// Same testing for running on YARN
        /// It sends close event for all the running tasks.
        /// </summary>
        [Fact(Skip = "Requires Yarn")]
        public void TestTaskCloseOnLocalRuntimeOnYarn()
        {
            const int chunkSize = 2;
            const int dims = 50;
            const int iterations = 200;
            const int mapperMemory = 512;
            const int updateTaskMemory = 512;
            const int numTasks = 4;
            const int numOfRetryInRecovery = 4;
            TestBroadCastAndReduce(true, numTasks, chunkSize, dims, iterations, mapperMemory, updateTaskMemory, numOfRetryInRecovery);
        }

        /// <summary>
        /// This method overrides base class method and defines its own event handlers for driver. 
        /// It uses its own RunningTaskHandler, FailedEvaluatorHandler and CompletedTaskHandler, FailedTaskHandler so that to simulate the test scenarios
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
                    GenericType<TestHandlers>.Class)
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
        /// Mapper function configuration. Add TcpConfiguration to the base configuration
        /// </summary>
        /// <returns></returns>
        protected override IConfiguration BuildMapperFunctionConfig()
        {
            return Configurations.Merge(GetTcpConfiguration(), base.BuildMapperFunctionConfig());
        }

        /// <summary>
        /// Update function configuration. Add TcpConfiguration to the base configuration.
        /// </summary>
        /// <returns></returns>
        protected override IConfiguration BuildUpdateFunctionConfig()
        {
            return Configurations.Merge(GetTcpConfiguration(), base.BuildUpdateFunctionConfig());
        }

        /// <summary>
        /// Override default setting for retry policy
        /// </summary>
        /// <returns></returns>
        private IConfiguration GetTcpConfiguration()
        {
            return TcpClientConfigurationModule.ConfigurationModule
                .Set(TcpClientConfigurationModule.MaxConnectionRetry, "5")
                .Set(TcpClientConfigurationModule.SleepTime, "1000")
                .Build();
        }

        /// <summary>
        /// Test handlers
        /// </summary>
        internal sealed class TestHandlers : IObserver<IRunningTask>, IObserver<ICompletedTask>, IObserver<IFailedTask>, IObserver<IFailedEvaluator>
        {
            private readonly ISet<IRunningTask> _runningTasks = new HashSet<IRunningTask>();
            private readonly object _lock = new object();

            [Inject]
            private TestHandlers()
            {
            }

            /// <summary>
            /// Add the RunningTask to _runningTasks and dispose the last received running task
            /// </summary>
            public void OnNext(IRunningTask value)
            {
                lock (_lock)
                {
                    Logger.Log(Level.Info, "Received running task:" + value.Id);
                    _runningTasks.Add(value);
                    if (_runningTasks.Count == 4)
                    {
                        Logger.Log(Level.Info, "Dispose running task from driver:" + value.Id);
                        value.Dispose(ByteUtilities.StringToByteArrays(TaskManager.CloseTaskByDriver));
                        _runningTasks.Remove(value);
                    }
                }
            }

            /// <summary>
            /// Log the task id and FailTaskMessage
            /// Close the rest of the running tasks, then dispose the context
            /// </summary>
            /// <param name="value"></param>
            public void OnNext(IFailedTask value)
            {
                lock (_lock)
                {
                    Logger.Log(Level.Info, FailedTaskMessage + value.Id);
                    CloseRunningTasks();
                    value.GetActiveContext().Value.Dispose();
                }
            }

            /// <summary>
            /// No Failed Evaluator is expected
            /// </summary>
            /// <param name="value"></param>
            public void OnNext(IFailedEvaluator value)
            {
                throw new Exception(FailedEvaluatorMessage);
            }

            /// <summary>
            /// Log the task id and ICompletedTask
            /// Close the rest of the running tasks, then dispose the context
            /// </summary>
            public void OnNext(ICompletedTask value)
            {
                lock (_lock)
                {
                    Logger.Log(Level.Info, CompletedTaskMessage + value.Id);
                    CloseRunningTasks();
                    value.ActiveContext.Dispose();
                }
            }

            private void CloseRunningTasks()
            {
                foreach (var task in _runningTasks)
                {
                    Logger.Log(Level.Info, "Dispose running task from driver:" + task.Id);
                    task.Dispose(ByteUtilities.StringToByteArrays(TaskManager.CloseTaskByDriver));
                }
                _runningTasks.Clear();
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