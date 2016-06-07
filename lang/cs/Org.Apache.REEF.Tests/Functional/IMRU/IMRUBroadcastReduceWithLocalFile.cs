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

using System.Collections.Generic;
using Org.Apache.REEF.IO.PartitionedData.FileSystem;
using Org.Apache.REEF.Tang.Interface;
using Xunit;

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
                0,
                testFolder);
            ValidateSuccessForLocalRuntime(numTasks, 0, 0, testFolder, 100);
            CleanUp(testFolder);
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
    }
}
