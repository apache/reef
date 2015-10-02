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
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.FileSystem.Hadoop;
using Org.Apache.REEF.Tang.Implementations.Tang;

namespace Org.Apache.REEF.IO.TestClient
{
    class HadoopFileSystemTest
    {
        public IFileSystem _fileSystem;

        internal HadoopFileSystemTest()
        {
            _fileSystem = 
                TangFactory.GetTang()
                .NewInjector(HadoopFileSystemConfiguration.ConfigurationModule.Build())
                .GetInstance<IFileSystem>();
        }

        private Uri GetTempUri()
        {
            return
                new Uri(_fileSystem.UriPrefix + "vol1/test/TestHadoopFileSystem-" +
                        DateTime.Now.ToString("yyyyMMddHHmmssfff"));
        }

        internal void TestCopyFromLocalAndBack()
        {
            var localFile = MakeLocalBanaryFile();
            Console.WriteLine("localFile: " + localFile);

            var localFileDownloaded = localFile + ".2";
            Console.WriteLine("localFileDownloaded: " + localFileDownloaded);

            var remoteUri = GetTempUri();
            Console.WriteLine("remoteUri: " + remoteUri);

            _fileSystem.CopyFromLocal(localFile, remoteUri);
            Console.WriteLine("File CopyFromLocal!");

            _fileSystem.CopyToLocal(remoteUri, localFileDownloaded);
            Console.WriteLine("File CopyToLocal!");

            if (!File.Exists(localFileDownloaded))
            {
                Console.WriteLine("File does not exist!");
            }
            else
            {
                Console.WriteLine("File copied! - " + localFile);
            }

            ReadFileTest(localFileDownloaded);

            Console.WriteLine("End of TestCopyFromLocalAndBack2!");

            _fileSystem.Delete(remoteUri);
            File.Delete(localFile);
            File.Delete(localFileDownloaded);
        }

        internal void TestCopyFromRemote()
        {
            //var remoteUri = new Uri(_fileSystem.GetUriPrefix() + "vol1/public/libraries/Scope/Data/Ads/1Month/stat.txt");
            //var remoteUri = new Uri(_fileSystem.UriPrefix + "vol1/public/libraries/Scope/Data/Ads/1Month/test20150925145717050.txt");
            var remoteUri = new Uri(_fileSystem.UriPrefix + "vol1/test/TestHadoopFilePartition-20151002160654404");
            
            Console.WriteLine("remoteUri: " + remoteUri);

            if (!_fileSystem.Exists(remoteUri))
            {
                Console.WriteLine("Remote File does not exists.");
            }
            else
            {
                Console.WriteLine("Remote File found exists." + remoteUri);
            }

            var localFile = Path.GetTempPath() + "-" + DateTime.Now.ToString("yyyyMMddHHmmssfff");
            Console.WriteLine("localFile: " + localFile);

            _fileSystem.CopyToLocal(remoteUri, localFile);
            Console.WriteLine("File CopyToLocal!");
        }

        private string MakeLocalBanaryFile()
        {
            var result = Path.GetTempFileName();
            WriteFileTest(result);
            return result;
        }

        private void WriteFileTest(string filePath)
        {
            int inputCount = 1;
            using (FileStream fs = File.Open(filePath, FileMode.Create))
            {
                BinaryWriter writer = new BinaryWriter(fs);
                string strContent = inputCount + " -- firstpart";
                writer.Write(strContent);

                strContent = inputCount++ + " -- secondpart";
                writer.Write(strContent);

                writer.Write(1.250F);
                writer.Write(@"c:\Temp");
                writer.Write(10);
                writer.Write(true);

                writer.Flush();
            }
        }

        private void ReadFileTest(string filePath)
        {
            using (FileStream fs = File.Open(filePath, FileMode.Open))
            {
                BinaryReader reader = new BinaryReader(fs);

                string strContent1 = reader.ReadString();
                string strContent2 = reader.ReadString();

                float aspectRatio = reader.ReadSingle();
                string tempDirectory = reader.ReadString();
                int autoSaveTime = reader.ReadInt32();
                bool showStatusBar = reader.ReadBoolean();

                Console.WriteLine("strContent1: " + strContent1);
                Console.WriteLine("strContent2: " + strContent2);
                Console.WriteLine("Aspect ratio set to: " + aspectRatio);
                Console.WriteLine("Temp directory is: " + tempDirectory);
                Console.WriteLine("Auto save time set to: " + autoSaveTime);
                Console.WriteLine("Show status bar: " + showStatusBar);
            }
        }
    }
}
