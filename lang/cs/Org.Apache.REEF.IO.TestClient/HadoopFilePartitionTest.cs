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
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.FileSystem.Hadoop;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.IO.PartitionedData.FileSystem;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IO.TestClient
{
    internal class HadoopFilePartitionTest
    {
        internal static void TestWithByteDeserializer()
        {
            string remoteFilePath1 = MakeRemoteTestFile(new byte[] { 111, 112, 113 });
            string remoteFilePath2 = MakeRemoteTestFile(new byte[] { 114, 115, 116, 117 });

            var serializerConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation<IFileSerializer<byte>, ByteSerializer>(GenericType<IFileSerializer<byte>>.Class,
                    GenericType<ByteSerializer>.Class)
                .Build();
            var serializerConfString = (new AvroConfigurationSerializer()).ToString(serializerConf);

            var dataSet = TangFactory.GetTang()
                .NewInjector(FileSystemPartitionConfiguration<byte>.ConfigurationModule
                    .Set(FileSystemPartitionConfiguration<byte>.FilePaths, remoteFilePath1)
                    .Set(FileSystemPartitionConfiguration<byte>.FilePaths, remoteFilePath2)
                    .Set(FileSystemPartitionConfiguration<byte>.FileSerializerConfig, serializerConfString)
                .Build(),
                  HadoopFileSystemConfiguration.ConfigurationModule.Build())
                .GetInstance<IPartitionedDataSet>();
       
            Console.WriteLine("IPartitionedDataSet created. ");
            int count = 0;
            foreach (var partitionDescriptor in dataSet)
            {
                var partition =
                    TangFactory.GetTang()
                        .NewInjector(partitionDescriptor.GetPartitionConfiguration())
                        .GetInstance<IPartition<IEnumerable<byte>>>();

                Console.WriteLine("get partition instance.");

                var e = partition.GetPartitionHandle();
                foreach (var v in e)
                {
                    Console.WriteLine(v);
                    count++;
                }
            }
            Console.WriteLine("Total count returend: " + count);            
        }

        internal static string MakeRemoteTestFile(byte[] bytes)
        {
            IFileSystem fileSystem =
                TangFactory.GetTang()
                .NewInjector(HadoopFileSystemConfiguration.ConfigurationModule.Build())
                .GetInstance<IFileSystem>();

            string localFile = MakeLocalTempFile(bytes);

            string remoteFileName = "vol1/test/TestHadoopFilePartition-" +
                                    DateTime.Now.ToString("yyyyMMddHHmmssfff");

            var remoteUri = new Uri(fileSystem.UriPrefix + remoteFileName);
            Console.WriteLine("remoteUri: " + remoteUri);

            fileSystem.CopyFromLocal(localFile, remoteUri);
            Console.WriteLine("File CopyFromLocal!");

            return remoteFileName;
        }

        internal static void MakeLocalTestFile(string filePath, byte[] bytes)
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

        internal static string MakeLocalTempFile(byte[] bytes)
        {
            var result = Path.GetTempFileName();
            MakeLocalTestFile(result, bytes);
            return result;
        }
    }

    internal class ByteSerializer : IFileSerializer<byte>
    {
        [Inject]
        private ByteSerializer()
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
}
