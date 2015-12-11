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
using System.Globalization;
using System.IO;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.FileSystem.Hadoop;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IO.TestClient
{
    class HadoopFileSystemTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(HadoopFileSystemTest));

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
                _fileSystem.CreateUriForPath("/tmp/TestHadoopFileSystem-" +
                        Guid.NewGuid().ToString("N").Substring(0, 8));
        }

        internal bool TestCopyFromLocalAndBack()
        {
            bool result = false;

            var localFile = MakeLocalBanaryFile();
            Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "localFile {0}: ", localFile));

            var localFileDownloaded = localFile + ".2";
            var remoteUri = GetTempUri();
            Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "remoteUri {0}: ", remoteUri.AbsolutePath));

            _fileSystem.CopyFromLocal(localFile, remoteUri);
            Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "File CopyFromLocal {0}: ", remoteUri));

            _fileSystem.CopyToLocal(remoteUri, localFileDownloaded);
            Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "File CopyToLocal {0}: ", localFileDownloaded));

            if (File.Exists(localFileDownloaded))
            {
                Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "File copied exists {0}: ", localFileDownloaded));
                ReadFileTest(localFileDownloaded);
                File.Delete(localFileDownloaded);
                result = true;
            }
            else
            {
                Logger.Log(Level.Info, "File does not exist!");
                result = false;
            }

            _fileSystem.Delete(remoteUri);
            File.Delete(localFile);
            Logger.Log(Level.Info, "End of TestCopyFromLocalAndBack2!");

            return result;
        }

        internal bool TestCopyFromRemote(string path)
        {
            var remoteUri = _fileSystem.CreateUriForPath(path);
            Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "remoteUri {0}: ", remoteUri));

            return CopyRemoteToLocal(remoteUri);
        }

        private bool CopyRemoteToLocal(Uri remoteUri)
        {
            if (!_fileSystem.Exists(remoteUri))
            {
                Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "Remote File {0} doesn't exist.", remoteUri));
                return false;
            }
            Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "Remote File {0} found exists.", remoteUri));

            var localFile = Path.GetTempPath() + "-" + DateTime.Now.ToString("yyyyMMddHHmmssfff");
            Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "localFile {0}.", localFile));

            _fileSystem.CopyToLocal(remoteUri, localFile);
            Logger.Log(Level.Info, "File CopyToLocal!");

            return true;
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

                Logger.Log(Level.Info, "strContent1: " + strContent1);
                Logger.Log(Level.Info, "strContent2: " + strContent2);
                Logger.Log(Level.Info, "Aspect ratio set to: " + aspectRatio);
                Logger.Log(Level.Info, "Temp directory is: " + tempDirectory);
                Logger.Log(Level.Info, "Auto save time set to: " + autoSaveTime);
                Logger.Log(Level.Info, "Show status bar: " + showStatusBar);
            }
        }
    }
}
