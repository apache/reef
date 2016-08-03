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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.IO.PartitionedData.FileSystem;
using Org.Apache.REEF.IO.TempFileCreation;
using Org.Apache.REEF.Network.Examples.GroupCommunication.BroadcastReduceDriverAndTasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;
using TraceLevel = System.Diagnostics.TraceLevel;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    /// <summary>
    /// This class tests DataLoadingContext for IMRU with FilePartitionDataSet
    /// </summary>
    [Collection("FunctionalTests")]
    public class IMRUBroadcastReduceWithFilePartitionDataSetTest : IMRUBrodcastReduceTestBase
    {
        /// <summary>
        /// Defines the number of data in input data bytes in the test
        /// </summary>
        private const int DataCount = 3;

        /// <summary>
        /// This test tests DataLoadingContext with FilePartitionDataSet
        /// The IPartitionedInputDataSet configured in this test is passed to IMRUDriver, and IInputPartition associated with it is injected in 
        /// DataLoadingContext for the Evaluator. Cache method in IInputPartition is called when the context starts. 
        /// Most of the validation of the test is done inside test Mapper function.
        /// It will verify if the temp file downloaded exists before calling IInputPartition.GetPartitionHandle()
        /// </summary>
        [Fact]
        public void TestWithFilePartitionDataSetOnLocalRuntime()
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
            ////CleanUp(testFolder);
        }

        /// <summary>
        /// This method overrides base class method to pass IEnumerable<Row> as TPartitionType
        /// </summary>
        protected new void TestBroadCastAndReduce(bool runOnYarn,
            int numTasks,
            int chunkSize,
            int dims,
            int iterations,
            int mapperMemory,
            int updateTaskMemory,
            string testFolder = DefaultRuntimeFolder)
        {
            string runPlatform = runOnYarn ? "yarn" : "local";
            TestRun(DriverConfiguration<int[], int[], int[], IEnumerable<Row>>(
                CreateIMRUJobDefinitionBuilder(numTasks - 1, chunkSize, iterations, dims, mapperMemory, updateTaskMemory),
                DriverEventHandlerConfigurations<int[], int[], int[], IEnumerable<Row>>()),
                typeof(BroadcastReduceDriver),
                numTasks,
                "BroadcastReduceDriver",
                runPlatform,
                testFolder);
        }    

        /// <summary>
        /// This method defines event handlers for driver. As default, it uses all the handlers defined in IMRUDriver.
        /// </summary>
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
                .Set(REEF.Driver.DriverConfiguration.OnTaskFailed,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.CustomTraceLevel, TraceLevel.Info.ToString())
                .Build();
        }

        /// <summary>
        /// This test case tests PartitionedDataset Configuration built in this class for IPartitionedInputDataSet and IInputPartition
        /// </summary>
        [Fact]
        public void TestBuildPartitionedDatasetConfiguration()
        {
            int count = 0;
            int numberOfTasks = 4;

            var dataSet = TangFactory.GetTang().NewInjector(BuildPartitionedDatasetConfiguration(numberOfTasks))
                .GetInstance<IPartitionedInputDataSet>();

            foreach (var partitionDescriptor in dataSet)
            {
                var partition =
                    TangFactory.GetTang()
                        .NewInjector(partitionDescriptor.GetPartitionConfiguration())
                        .GetInstance<IInputPartition<IEnumerable<Row>>>();

                using (partition as IDisposable)
                {
                    IEnumerable<Row> e = partition.GetPartitionHandle();

                    foreach (var row in e)
                    {
                        Logger.Log(Level.Info, "Data read {0}: ", row.GetValue());
                        count++;
                    }
                }
            }

            Assert.Equal(count, numberOfTasks * DataCount);
        }

        /// <summary>
        /// Mapper function configuration.
        /// </summary>
        /// <returns></returns>
        protected override IConfiguration BuildMapperFunctionConfig()
        {
            return IMRUMapConfiguration<int[], int[]>.ConfigurationModule
                .Set(IMRUMapConfiguration<int[], int[]>.MapFunction,
                    GenericType<TestSenderMapFunction<IEnumerable<Row>>>.Class)
                .Build();
        }

        /// <summary>
        /// Test Mapper function. It will access IInputPartition.
        /// It verifies that the data file has been downloaded to temp folder
        /// It then iterates the data and verifies the data count. 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        internal sealed class TestSenderMapFunction<T> : IMapFunction<int[], int[]>
        {
            private int _iterations;

            [Inject]
            private TestSenderMapFunction(
                [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
                IInputPartition<T> partition)
            {
                var tempFileDir = TangFactory.GetTang().NewInjector().GetNamedInstance<TempFileFolder, string>();
                var tmpFileFodler = Directory.GetCurrentDirectory() + tempFileDir.Substring(1, tempFileDir.Length - 1);
                Assert.True(Directory.Exists(tmpFileFodler));
                
                var directories = Directory.EnumerateDirectories(tmpFileFodler);
                Assert.Equal(1, directories.Count());

                var directory = directories.FirstOrDefault();
                Assert.True(directory.Contains("-partition-"));

                var files = Directory.EnumerateFiles(directory);
                Assert.Equal(1, files.Count());
                var file = files.FirstOrDefault();
                var a = file.Split('\\');
                var fileName = a[a.Length - 1];
                Assert.Equal(8, fileName.Length);

                var matchCounter = Regex.Matches("40af4c53", @"[a-zA-Z0-9]").Count;
                Assert.Equal(8, matchCounter);

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

        /// <summary>
        /// This method builds partition dataset configuration. It uses local file system and test RowSerializer
        /// </summary>
        /// <param name="numberofMappers"></param>
        /// <returns></returns>
        protected override IConfiguration BuildPartitionedDatasetConfiguration(int numberofMappers)
        {
            const string tempFileName = "REEF.TestLocalFileSystem.tmp";
            string sourceFilePath = Path.Combine(Path.GetTempPath(), tempFileName);

            var cm = FileSystemInputPartitionConfiguration<IEnumerable<Row>>.ConfigurationModule;

            for (var i = 0; i < numberofMappers; i++)
            {
                MakeLocalTestFile(sourceFilePath + i, CreateTestData(DataCount));
                cm = cm.Set(FileSystemInputPartitionConfiguration<IEnumerable<Row>>.FilePathForPartitions, sourceFilePath + i);
            }
            
            return cm.Set(FileSystemInputPartitionConfiguration<IEnumerable<Row>>.CopyToLocal, "true")
                     .Set(FileSystemInputPartitionConfiguration<IEnumerable<Row>>.FileSerializerConfig, GetRowSerializerConfigString())
                     .Build();
        }

        /// <summary>
        /// Create test data as bytes
        /// </summary>
        /// <param name="dataNumber"></param>
        /// <returns></returns>
        private static byte[] CreateTestData(int dataNumber)
        {
            var bytes = new byte[dataNumber];
            for (int i = 0; i < dataNumber; i++)
            {
                bytes[i] = (byte)(i + 'a');
            }
            return bytes;
        }

        /// <summary>
        /// Create a test data file as input file
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="bytes"></param>
        private static void MakeLocalTestFile(string filePath, byte[] bytes)
        {
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }

            using (var s = File.Create(filePath))
            {
                foreach (var b in bytes)
                {
                    s.WriteByte(b);
                }
            }
        }

        /// <summary>
        /// Test DeSerializer
        /// </summary>
        private class RowSerializer : IFileDeSerializer<IEnumerable<Row>>
        {
            [Inject]
            private RowSerializer()
            {
            }

            /// <summary>
            /// read all the files in the set and return byte read one by one
            /// </summary>
            /// <param name="filePaths"></param>
            /// <returns></returns>
            public IEnumerable<Row> Deserialize(ISet<string> filePaths)
            {
                foreach (var f in filePaths)
                {
                    using (FileStream stream = File.Open(f, FileMode.Open))
                    {
                        BinaryReader reader = new BinaryReader(stream);
                        while (reader.PeekChar() != -1)
                        {
                            yield return new Row(reader.ReadByte());
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Build RowSerialzer configuration and serialize it as string so that to pass it to driver
        /// </summary>
        /// <returns></returns>
        private static string GetRowSerializerConfigString()
        {
            var serializerConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(
                    GenericType<IFileDeSerializer<IEnumerable<Row>>>.Class,
                    GenericType<RowSerializer>.Class)
                .Build();
            return (new AvroConfigurationSerializer()).ToString(serializerConf);
        }

        /// <summary>
        /// Test Row class
        /// </summary>
        private class Row
        {
            private readonly byte _b;

            public Row(byte b)
            {
                _b = b;
            }

            public byte GetValue()
            {
                return _b;
            }
        }
    }
}
