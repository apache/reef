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
using System.Globalization;
using System.IO;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.FileSystem.Hadoop;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.IO.PartitionedData.FileSystem;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IO.TestClient
{
    // TODO[JIRA REEF-815]: once move to Nunit, tose test should be moved to Test project
    internal class HadoopFileInputPartitionTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(HadoopFileInputPartitionTest));

        internal static bool TestWithByteDeserializer()
        {
            string remoteFilePath1 = MakeRemoteTestFile(new byte[] { 111, 112, 113 });
            string remoteFilePath2 = MakeRemoteTestFile(new byte[] { 114, 115, 116, 117 });

            var serializerConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation<IFileDeSerializer<IEnumerable<byte>>, ByteSerializer>(GenericType<IFileDeSerializer<IEnumerable<byte>>>.Class,
                    GenericType<ByteSerializer>.Class)
                .Build();
            var serializerConfString = (new AvroConfigurationSerializer()).ToString(serializerConf);

            var dataSet = TangFactory.GetTang()
                .NewInjector(FileSystemInputPartitionConfiguration<IEnumerable<byte>>.ConfigurationModule
                    .Set(FileSystemInputPartitionConfiguration<IEnumerable<byte>>.FilePathForPartitions, remoteFilePath1)
                    .Set(FileSystemInputPartitionConfiguration<IEnumerable<byte>>.FilePathForPartitions, remoteFilePath2)
                    .Set(FileSystemInputPartitionConfiguration<IEnumerable<byte>>.FileSerializerConfig, serializerConfString)
                    .Set(FileSystemInputPartitionConfiguration<IEnumerable<byte>>.TempFileFolder, @".\reef\mytest\")
                .Build(),
                  HadoopFileSystemConfiguration.ConfigurationModule.Build())
                .GetInstance<IPartitionedInputDataSet>();

            Logger.Log(Level.Info, "IPartitionedDataSet created.");

            int count = 0;
            foreach (var partitionDescriptor in dataSet)
            {
                var partition = TangFactory.GetTang()
                    .NewInjector(partitionDescriptor.GetPartitionConfiguration(), GetHadoopFileSystemConfiguration())
                    .GetInstance<IInputPartition<IEnumerable<byte>>>();

                Logger.Log(Level.Info, "GetInstance of partition.");
                using (partition as IDisposable)
                {
                    Logger.Log(Level.Info, "get partition instance.");

                    var e = partition.GetPartitionHandle();
                    foreach (var v in e)
                    {
                        Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "Data read {0}: ", v));
                        count++;
                    }
                }
            }
            Logger.Log(Level.Info, "Total count returend: " + count);
            return true;
        }

        internal static string MakeRemoteTestFile(byte[] bytes)
        {
            IFileSystem fileSystem =
                TangFactory.GetTang()
                .NewInjector(HadoopFileSystemConfiguration.ConfigurationModule.Build())
                .GetInstance<IFileSystem>();

            string localFile = MakeLocalTempFile(bytes);

            string remoteFileName = "/tmp/TestHadoopFilePartition-" +
                                    DateTime.Now.ToString("yyyyMMddHHmmssfff");

            var remoteUri = fileSystem.CreateUriForPath(remoteFileName);
            Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "remoteUri {0}: ", remoteUri));

            fileSystem.CopyFromLocal(localFile, remoteUri);
            Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "File CopyFromLocal {0}: ", localFile));

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
                {
                    s.WriteByte(b);
                }
            }
        }

        internal static string MakeLocalTempFile(byte[] bytes)
        {
            var result = Path.GetTempFileName();
            MakeLocalTestFile(result, bytes);
            return result;
        }

        private static IConfiguration GetHadoopFileSystemConfiguration()
        {
            return TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(typeof(IFileSystem), typeof(HadoopFileSystem))
                .Build();
        }
    }

    internal class ByteSerializer : IFileDeSerializer<IEnumerable<byte>>
    {
        [Inject]
        private ByteSerializer()
        { 
        }

        /// <summary>
        /// Read bytes from all the files in the file folder and return one by one
        /// </summary>
        /// <param name="fileFolder"></param>
        /// <returns></returns>
        public IEnumerable<byte> Deserialize(string fileFolder)
        {
            foreach (var f in Directory.GetFiles(fileFolder))
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
