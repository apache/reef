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

using System.Diagnostics;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    [Collection("FunctionalTests")]
    public class IMRUBroadcastReduceJobCancelledTest : IMRUBrodcastReduceTestBase
    {
        /// <summary>
        /// This test is for the cancellation scenario for IMRUDriver on local runtime
        /// </summary>
        [Fact]
        public void TestBroadcastReduceJobWithJobCancellation()
        {
            int chunkSize = 2;
            int dims = 10;
            int iterations = 30;
            int mapperMemory = 5120;
            int updateTaskMemory = 5120;
            int numTasks = 14;
            int numberOfRetryInRecovery = 4;
            string testFolder = DefaultRuntimeFolder + TestId;
            TestBroadCastAndReduce(false, numTasks, chunkSize, dims, iterations, mapperMemory, updateTaskMemory, numberOfRetryInRecovery, testFolder, numberOfChecksBeforeCancellingJob: 0);
            ValidateSuccessForLocalRuntime(null, 0, 0, testFolder, expectedCancellationMessage: "cancellation message: IsCancelled check count: 1");
            CleanUp(testFolder);
        }

        /// <summary>
        /// This method defines event handlers for driver. As default, it uses all the handlers defined in IMRUDriver.
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
                     GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnEvaluatorFailed,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnContextFailed,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskRunning,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskFailed,
                     GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.CustomTraceLevel, TraceLevel.Info.ToString())
                .Build();
        }
    }
}
