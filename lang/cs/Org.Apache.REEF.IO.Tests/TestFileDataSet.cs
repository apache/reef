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
using System.Collections.Generic;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.IO.PartitionedData.FileSystem;
using Org.Apache.REEF.IO.PartitionedData.FileSystem.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IO.Tests
{
    /// <summary>
    /// Tests for Org.Apache.REEF.IO.PartitionedData.FileSystem.
    /// </summary>
    [TestClass]
    public class TestFileDataSet
    {
        /// <remarks>
        /// This test creates IPartitionDataSet with FileSystemPartitionConfiguration module.
        /// It then instantiates each IPartition using the IConfiguration provided by the IPartitionDescriptor.
        /// </remarks>
        [TestMethod]
        public void TestEvaluatorSide()
        {
            var c = GetByteSerilizerConfigString();
            var dataSet = TangFactory.GetTang()
                .NewInjector(FileSystemPartitionConfiguration<byte>.ConfigurationModule
                    .Set(FileSystemPartitionConfiguration<byte>.FilePaths, "abc;ppp")
                    .Set(FileSystemPartitionConfiguration<byte>.FilePaths, "def;ooo;iii")
                    .Set(FileSystemPartitionConfiguration<byte>.FileSerializerConfig, c)
                    .Build())
                .GetInstance<IPartitionedDataSet>();

            foreach (var partitionDescriptor in dataSet)
            {
                var partition =
                    TangFactory.GetTang()
                        .NewInjector(partitionDescriptor.GetPartitionConfiguration())
                        .GetInstance<IPartition<IEnumerable<byte>>>();
                Assert.IsNotNull(partition);
                Assert.IsNotNull(partition.Id);
            }
        }

        /// <remarks>
        /// This test creates IPartitionDataSet using the configuration build directly.
        /// It sets multiple files in each Partition
        /// </remarks>
        [TestMethod]
        public void TestWithoutConfigurationModule()
        {
            var c = GetByteSerilizerConfigString();

            var partitionConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IPartitionedDataSet>.Class, GenericType<FileSystemDataSet<byte>>.Class)
                .BindStringNamedParam<FileSerializerConfigString>(c)
                .BindSetEntry<AllFilePaths, string>(GenericType<AllFilePaths>.Class, "www;ttt")
                .BindSetEntry<AllFilePaths, string>(GenericType<AllFilePaths>.Class, "eee")
                .Build();

            var dataSet = TangFactory.GetTang()
                .NewInjector(partitionConfig)
                .GetInstance<IPartitionedDataSet>();

            foreach (var partitionDescriptor in dataSet)
            {
                var partition =
                    TangFactory.GetTang()
                        .NewInjector(partitionDescriptor.GetPartitionConfiguration())
                        .GetInstance<IPartition<IEnumerable<byte>>>();
                Assert.IsNotNull(partition);
                Assert.IsNotNull(partition.Id);
            }
        }

        /// <remarks>
        /// This test is to use a ByteSerializer.
        /// </remarks>
        [TestMethod]
        public void TestWithByteDeserializer()
        {
            const string tempFileName1 = "REEF.TestLocalFileSystem1.tmp";
            const string tempFileName2 = "REEF.TestLocalFileSystem2.tmp";
            var sourceFilePath1 = Path.Combine(Path.GetTempPath(), tempFileName1);
            var sourceFilePath2 = Path.Combine(Path.GetTempPath(), tempFileName2);

            MakeLocalTestFile(sourceFilePath1, new byte[] { 111, 112, 113 });
            MakeLocalTestFile(sourceFilePath2, new byte[] { 114, 115, 116, 117 });

            var c = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IPartitionedDataSet>.Class, GenericType<FileSystemDataSet<byte>>.Class)
                .BindStringNamedParam<FileSerializerConfigString>(GetByteSerilizerConfigString())
                .BindSetEntry<AllFilePaths, string>(GenericType<AllFilePaths>.Class, sourceFilePath1)
                .BindSetEntry<AllFilePaths, string>(GenericType<AllFilePaths>.Class, sourceFilePath2)
                .Build();

            var dataSet = TangFactory.GetTang()
                .NewInjector(c)
                .GetInstance<IPartitionedDataSet>();
            
            int count = 0;
            foreach (var partitionDescriptor in dataSet)
            {
                var partition =
                    TangFactory.GetTang()
                        .NewInjector(partitionDescriptor.GetPartitionConfiguration())
                        .GetInstance<IPartition<IEnumerable<byte>>>();

                var e = partition.GetPartitionHandle();
                foreach (var v in e)
                {
                    Console.WriteLine(v);
                    count++;
                }
            }
            Assert.AreEqual(count, 7);
        }

        /// <remarks>
        /// This test is to use a RowSerializer. Row is a class at client side. 
        /// </remarks>
        [TestMethod]
        public void TestWithRowDeserializer()
        {
            const string tempFileName1 = "REEF.TestLocalFileSystem1.tmp";
            const string tempFileName2 = "REEF.TestLocalFileSystem2.tmp";
            var sourceFilePath1 = Path.Combine(Path.GetTempPath(), tempFileName1);
            var sourceFilePath2 = Path.Combine(Path.GetTempPath(), tempFileName2);

            MakeLocalTestFile(sourceFilePath1, new byte[] {111, 112, 113});
            MakeLocalTestFile(sourceFilePath2, new byte[] { 114, 115 });

            var dataSet = TangFactory.GetTang()
                .NewInjector(FileSystemPartitionConfiguration<Row>.ConfigurationModule
                    .Set(FileSystemPartitionConfiguration<Row>.FilePaths, sourceFilePath1)
                    .Set(FileSystemPartitionConfiguration<Row>.FilePaths, sourceFilePath2)
                    .Set(FileSystemPartitionConfiguration<Row>.FileSerializerConfig, GetRowSerilizerConfigString())
                    .Build())
                .GetInstance<IPartitionedDataSet>();

            int count = 0;
            foreach (var partitionDescriptor in dataSet)
            {
                var partition =
                    TangFactory.GetTang()
                        .NewInjector(partitionDescriptor.GetPartitionConfiguration())
                        .GetInstance<IPartition<IEnumerable<Row>>>();

                IEnumerable<Row> e = partition.GetPartitionHandle();

                foreach (var row in e)
                {
                    Console.WriteLine(row.GetValue());
                    count++;
                }
            }
            Assert.AreEqual(count, 5);
        }

        private void MakeLocalTestFile(string filePath, byte[] bytes)
        {
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }

            using (var s = File.Create(filePath))
            {
                foreach (var b in bytes)
                    s.WriteByte(b);
            }
        }

        private string GetByteSerilizerConfigString()
        {
            var serializerConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation<IFileSerializer<byte>, ByteSerializer>(
                    GenericType<IFileSerializer<byte>>.Class,
                    GenericType<ByteSerializer>.Class)
                .Build();
            return (new AvroConfigurationSerializer()).ToString(serializerConf);
        }

        private string GetRowSerilizerConfigString()
        {
            var serializerConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation<IFileSerializer<Row>, RowSerializer>(
                    GenericType<IFileSerializer<Row>>.Class,
                    GenericType<RowSerializer>.Class)
                .Build();
            return (new AvroConfigurationSerializer()).ToString(serializerConf);
        }
    }

    public class ByteSerializer : IFileSerializer<byte>
    {
        [Inject]
        public ByteSerializer()
        { }

        public IEnumerable<byte> Deserialize(IList<string> filePaths)
        {
            foreach (var f in filePaths)
            {
                using (FileStream stream = File.Open(f, FileMode.Open))
                {
                    BinaryReader reader = new BinaryReader(stream);
                    while (reader.PeekChar() != -1)
                    {
                        yield return reader.ReadByte();
                    }
                }
            }
        }
    }

    public class Row
    {
        private byte _b;

        public Row(byte b)
        {
            _b = b;
        }

        public byte GetValue()
        {
            return _b;
        }
    }

    public class RowSerializer : IFileSerializer<Row>
    {
        [Inject]
        public RowSerializer()
        { }

        public IEnumerable<Row> Deserialize(IList<string> filePaths)
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
}
