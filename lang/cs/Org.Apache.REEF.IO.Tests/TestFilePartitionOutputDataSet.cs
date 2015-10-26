/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.IO.PartitionedData.FileSystem;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IO.Tests
{
    /// <summary>
    /// Tests for Org.Apache.REEF.IO.PartitionedData.FileSystem.
    /// </summary>
    [TestClass]
    public class TestFilePartitionOutputDataSet
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TestFilePartitionOutputDataSet));

        const string TempFileName1 = "REEF.TestLocalFileSystem1.tmp";
        const string TempFileName2 = "REEF.TestLocalFileSystem2.tmp";
        readonly string _sourceFilePath1 = Path.Combine(Path.GetTempPath(), TempFileName1);
        readonly string _sourceFilePath2 = Path.Combine(Path.GetTempPath(), TempFileName2);

        /// <remarks>
        /// This test is to test FilePartitionedOutputDataset
        /// with a stream over the file
        /// </remarks>
        [TestMethod]
        public void TestOutputWithStreamSerializer()
        {
            if (File.Exists(_sourceFilePath1))
            {
                File.Delete(_sourceFilePath1);
            }

            if (File.Exists(_sourceFilePath2))
            {
                File.Delete(_sourceFilePath2);
            }

            var dataSet = TangFactory.GetTang()
                .NewInjector(FileSystemOutputPartitionConfiguration<Stream>.ConfigurationModule
                    .Set(FileSystemOutputPartitionConfiguration<Stream>.FilePathForPartitions, _sourceFilePath1)
                    .Set(FileSystemOutputPartitionConfiguration<Stream>.FilePathForPartitions, _sourceFilePath2)
                    .Set(FileSystemOutputPartitionConfiguration<Stream>.FileSerializerConfig, GetStreamSerializerConfigString())
                    .Build())
                .GetInstance<IPartitionedOutputDataSet>();

            int counter = 0;
            foreach (var partitionDescriptor in dataSet)
            {
                var partition =
                    TangFactory.GetTang()
                        .NewInjector(partitionDescriptor.GetPartitionConfiguration())
                        .GetInstance<IOutputPartition<Stream>>();
                using (partition as IDisposable)
                {
                    var outputStream = partition.GetOutputReceiver();
                    outputStream.Write(new[] {Convert.ToByte(counter + 1), Convert.ToByte(counter + 10)}, 0, 2);
                    outputStream.Close();
                    counter++;
                }
            }

            CheckFile(_sourceFilePath1, 1, 10);
            CheckFile(_sourceFilePath2, 2, 11);
        }

        /// <remarks>
        /// This test is to test FilePartitionedOutputDataset
        /// that uses RowSerializer for the output 
        /// </remarks>
        [TestMethod]
        public void TestOutputWithRowSerializer()
        {
            if (File.Exists(_sourceFilePath1))
            {
                File.Delete(_sourceFilePath1);
            }

            if (File.Exists(_sourceFilePath2))
            {
                File.Delete(_sourceFilePath2);
            }

            var dataSet = TangFactory.GetTang()
                .NewInjector(FileSystemOutputPartitionConfiguration<IObserver<Row>>.ConfigurationModule
                    .Set(FileSystemOutputPartitionConfiguration<IObserver<Row>>.FilePathForPartitions, _sourceFilePath1)
                    .Set(FileSystemOutputPartitionConfiguration<IObserver<Row>>.FilePathForPartitions, _sourceFilePath2)
                    .Set(FileSystemOutputPartitionConfiguration<IObserver<Row>>.FileSerializerConfig, GetRowFileSerializerConfigString())
                    .Build())
                .GetInstance<IPartitionedOutputDataSet>();

            int counter = 0;
            foreach (var partitionDescriptor in dataSet)
            {
                var partition =
                    TangFactory.GetTang()
                        .NewInjector(partitionDescriptor.GetPartitionConfiguration())
                        .GetInstance<IOutputPartition<IObserver<Row>>>();
                using (partition as IDisposable)
                {
                    var observer = partition.GetOutputReceiver();

                    Row r = new Row(Convert.ToByte(counter + 1));
                    observer.OnNext(r);
                    r = new Row(Convert.ToByte(counter + 10));
                    observer.OnNext(r);
                    counter++;
                }
            }

            CheckFile(_sourceFilePath1, 1, 10);
            CheckFile(_sourceFilePath2, 2, 11);
        }

        private void CheckFile(string filename, byte res1, byte res2)
        {
            Assert.IsTrue(File.Exists(filename));

            using (var stream = new FileStream(filename, FileMode.Open, FileAccess.Read))
            {
                byte[] actRes = new byte[2];
                stream.Read(actRes, 0, 2);
                Assert.AreEqual(actRes[0], res1);
                Assert.AreEqual(actRes[1], res2);
            }
        }

        private string GetStreamSerializerConfigString()
        {
            var serializerConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IFileSerializer<Stream>>.Class,
                    GenericType<StreamSerializer>.Class)
                .Build();
            return (new AvroConfigurationSerializer()).ToString(serializerConf);
        }

        private string GetRowFileSerializerConfigString()
        {
            var serializerConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IFileSerializer<IObserver<Row>>>.Class,
                    GenericType<RowFileSerializer>.Class)
                .Build();
            return (new AvroConfigurationSerializer()).ToString(serializerConf);
        }
    }

    internal class RowObserver : IObserver<Row>
    {
        private readonly string _fileName;

        internal RowObserver(string fileName)
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
            File.Create(fileName).Close();
            _fileName = fileName;
        }

        public void OnNext(Row value)
        {
            using (var stream = new FileStream(_fileName, FileMode.Append, FileAccess.Write))
            {
                stream.Write(new[] {value.GetValue()}, 0, 1);
            }
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

    internal class RowFileSerializer : IFileSerializer<IObserver<Row>>
    {
        [Inject]
        private RowFileSerializer()
        { }

        /// <summary>
        /// Returns observer of row that outputs row to the file
        /// </summary>
        /// <param name="outputFile"></param>
        /// <returns></returns>
        public IObserver<Row> Serializer(string outputFile)
        {
            return new RowObserver(outputFile);
        }
    }

    internal class StreamSerializer : IFileSerializer<Stream>
    {
        [Inject]
        private StreamSerializer()
        { }

        /// <summary>
        /// Returns the stream on the file
        /// </summary>
        /// <param name="outputFile"></param>
        /// <returns></returns>
        public Stream Serializer(string outputFile)
        {
            if (File.Exists(outputFile))
            {
                File.Delete(outputFile);
            }
            return new FileStream(outputFile,FileMode.OpenOrCreate, FileAccess.Write);
        }
    }
}
