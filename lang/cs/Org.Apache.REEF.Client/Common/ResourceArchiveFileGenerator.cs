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
using System.IO;
using System.IO.Compression;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.Common
{
    /// <summary>
    /// Creates and returns zip file for assembled driver folder.
    /// </summary>
    internal sealed class ResourceArchiveFileGenerator : IResourceArchiveFileGenerator
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(ResourceArchiveFileGenerator));
        private readonly REEFFileNames _reefFileNames;

        [Inject]
        private ResourceArchiveFileGenerator(REEFFileNames reefFileNames)
        {
            _reefFileNames = reefFileNames;
        }

        public string CreateArchiveToUpload(string folderPath)
        {
            string archivePath = Path.Combine(folderPath, Path.GetDirectoryName(folderPath) + ".zip");
            string reefFolder = Path.Combine(folderPath, _reefFileNames.GetReefFolderName());
            if (!Directory.Exists(reefFolder))
            {
                Exceptions.Throw(new DirectoryNotFoundException("Cannot find directory " + reefFolder), Log);
            }

            if (File.Exists(archivePath))
            {
                Exceptions.Throw(new InvalidOperationException("Archive file already exists " + archivePath), Log);
            }

            ZipFile.CreateFromDirectory(reefFolder, archivePath);
            return archivePath;
        }
    }
}