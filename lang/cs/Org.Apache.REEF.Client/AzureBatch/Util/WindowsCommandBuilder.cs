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
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.AzureBatch.Util
{
    internal sealed class WindowsCommandBuilder : AbstractCommandBuilder
    {
        private static readonly string CommandPrefix = @"Add-Type -AssemblyName System.IO.Compression.FileSystem; " +
          "[System.IO.Compression.ZipFile]::ExtractToDirectory(\\\"$env:AZ_BATCH_TASK_WORKING_DIR\\" +
              AzureBatchFileNames.GetTaskJarFileName() + "\\\", " +
              "\\\"$env:AZ_BATCH_TASK_WORKING_DIR\\reef\\\");";
        private const string ClassPathSeparator = ";";
        private const string OsCommandFormat = "powershell.exe /c \"{0}\";";

        [Inject]
        private WindowsCommandBuilder(
            REEFFileNames fileNames,
            AzureBatchFileNames azureBatchFileNames) : base(fileNames, azureBatchFileNames,
                CommandPrefix, OsCommandFormat)
        {
        }

        protected override string GetDriverClasspath()
        {
            List<string> classpathList = new List<string>()
            {
                string.Format("{0}/{1}/*", _fileNames.GetReefFolderName(), _fileNames.GetLocalFolderName()),
                string.Format("{0}/{1}/*", _fileNames.GetReefFolderName(), _fileNames.GetGlobalFolderName())
            };

            return string.Format("'{0};'", string.Join(ClassPathSeparator, classpathList));
        }
    }
}
