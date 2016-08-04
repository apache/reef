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
using System.IO;
using System.Linq;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.IO.PartitionedData.FileSystem;
using Org.Apache.REEF.IO.TempFileCreation;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;
using TraceLevel = System.Diagnostics.TraceLevel;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    [Collection("FunctionalTests")]
    public class IMRUBroadcastReduceWithLocalFile : IMRUBroadcastReduceWithFilePartitionDataSetTest
    {
        /// <summary>
        /// This test tests DataLoadingContext with FilePartitionDataSet
        /// The IPartitionedInputDataSet configured in this test is passed to IMRUDriver, and IInputPartition associated with it is injected in 
        /// DataLoadingContext for the Evaluator. Cache method in IInputPartition is called when the context starts. 
        /// The local file is used in the test, so CopyToLocal will be set to false. The validation will be inside test Mapper function.
        /// </summary>
        [Fact]
        public void TestWithFilePartitionDataSetForLocalFile()
        {
            int chunkSize = 2;
            int dims = 10;
            int iterations = 10;
            int mapperMemory = 5120;
            int updateTaskMemory = 5120;
            int numTasks = 4;
            string testFolder = DefaultRuntimeFolder + TestId;
            TestBroadCastAndReduce(false,
                numTasks,
                chunkSize,
                dims,
                iterations,
                mapperMemory,
                updateTaskMemory,
                testFolder);
            ValidateSuccessForLocalRuntime(numTasks, 0, 0, testFolder);
            CleanUp(testFolder);
        }

        /// <summary>
        /// This test case tests PartitionedDataset Configuration built in this class for IPartitionedInputDataSet and IInputPartition
        /// </summary>
        [Fact]
        public void TestBuildPartitionedDatasetConfigurationLocalFile()
        {
            PartitionDatasetConfig();
        }

        /// <summary>
        /// Mapper function configuration.
        /// </summary>
        /// <returns></returns>
        protected override IConfiguration BuildMapperFunctionConfig()
        {
            return IMRUMapConfiguration<int[], int[]>.ConfigurationModule
                .Set(IMRUMapConfiguration<int[], int[]>.MapFunction,
                    GenericType<TestSenderMapFunctionForLocalFile<IEnumerable<Row>>>.Class)
                .Build();
        }

        /// <summary>
        /// This method builds partition dataset configuration. It uses local file and test RowSerializer.
        /// </summary>
        /// <param name="numberofMappers"></param>
        /// <returns></returns>
        protected override IConfiguration BuildPartitionedDatasetConfiguration(int numberofMappers)
        {
            var cm = CreateDatasetConfigurationModule(numberofMappers);

            return cm.Set(FileSystemInputPartitionConfiguration<IEnumerable<Row>>.CopyToLocal, "false")
                     .Set(FileSystemInputPartitionConfiguration<IEnumerable<Row>>.FileSerializerConfig, GetRowSerializerConfigString())
                     .Build();
        }

        /// <summary>
        /// Test Mapper function. It will access IInputPartition.
        /// It verifies that the data file is not downloaded to temp folder as we choose CopytoLocal false in the  configuration
        /// It then iterates the data and verifies the data count. 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        internal sealed class TestSenderMapFunctionForLocalFile<T> : IMapFunction<int[], int[]>
        {
            private int _iterations;

            [Inject]
            private TestSenderMapFunctionForLocalFile(
                [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
                IInputPartition<T> partition)
            {
                var tempFileDir = TangFactory.GetTang().NewInjector().GetNamedInstance<TempFileFolder, string>();
                var tmpFileFodler = Directory.GetCurrentDirectory() + tempFileDir.Substring(1, tempFileDir.Length - 1);
                Assert.True(Directory.Exists(tmpFileFodler));

                var directories = Directory.EnumerateDirectories(tmpFileFodler);
                Assert.Equal(0, directories.Count());

                int count = 0;
                var e = (IEnumerable<Row>)partition.GetPartitionHandle();

                foreach (var row in e)
                {
                    Logger.Log(Level.Info, "Data read {0}: ", row.GetValue());
                    count++;
                }

                Logger.Log(Level.Info, "TestSenderMapFunction: TaskId: {0}, count {1}", taskId.Length, count);
                Assert.Equal(count, DataCount);
            }

            /// <summary>
            /// Simple test Map function
            /// </summary>
            /// <param name="mapInput">integer array</param>
            /// <returns>The same integer array</returns>
            int[] IMapFunction<int[], int[]>.Map(int[] mapInput)
            {
                _iterations++;

                Logger.Log(Level.Info, "Received value {0} in iteration {1}.", mapInput[0], _iterations);

                if (mapInput[0] != _iterations)
                {
                    Exceptions.Throw(new Exception("Expected value in mappers different from actual value"), Logger);
                }
                return mapInput;
            }
        }
    }
}
