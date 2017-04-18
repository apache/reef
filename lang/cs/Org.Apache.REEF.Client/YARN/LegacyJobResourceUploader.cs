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
using System.Threading.Tasks;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.YARN.Parameters;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.Yarn
{
    /// <summary>
    /// Uploads assembled driver folder to DFS as an archive file.
    /// Uploading of DFS is done via calling into org.apache.reef.bridge.client.JobResourceUploader
    /// Uploading via Java is necessary to support Hadoop versions before v2.7.0 because
    /// we need to find modification time with accuracy of seconds to use archive as resource for YARNRM
    /// <see cref="Org.Apache.REEF.IO.FileSystem.Hadoop.HadoopFileSystem"/> implementation uses HDFS
    /// commandline shell which does not provide modification time with accuracy of seconds until v2.7.1
    /// </summary>
    internal sealed class LegacyJobResourceUploader : IJobResourceUploader
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(LegacyJobResourceUploader));

        private static readonly string JavaClassNameForResourceUploader =
            @"org.apache.reef.bridge.client.JobResourceUploader";

        private readonly IJavaClientLauncher _javaLauncher;
        private readonly IResourceArchiveFileGenerator _resourceArchiveFileGenerator;
        private readonly IFile _file;
        private readonly REEFFileNames _reefFileNames;

        [Inject]
        private LegacyJobResourceUploader(
            IJavaClientLauncher javaLauncher,
            IResourceArchiveFileGenerator resourceArchiveFileGenerator,
            IFile file,
            IYarnCommandLineEnvironment yarn,
            REEFFileNames reefFileNames)
        {
            _file = file;
            _resourceArchiveFileGenerator = resourceArchiveFileGenerator;
            _javaLauncher = javaLauncher;
            _javaLauncher.AddToClassPath(yarn.GetYarnClasspathList());
            _reefFileNames = reefFileNames;
        }

        public async Task<JobResource> UploadArchiveResourceAsync(string driverLocalFolderPath, string remoteUploadDirectoryPath)
        {
            driverLocalFolderPath = driverLocalFolderPath.TrimEnd('\\') + @"\";
            var driverUploadPath = remoteUploadDirectoryPath.TrimEnd('/') + @"/";
            Log.Log(Level.Info, "DriverFolderPath: {0} DriverUploadPath: {1}", driverLocalFolderPath, driverUploadPath);

            var archivePath = _resourceArchiveFileGenerator.CreateArchiveToUpload(driverLocalFolderPath);
            return await UploadResourceAndGetInfoAsync(archivePath, ResourceType.ARCHIVE, driverUploadPath, _reefFileNames.GetReefFolderName());
        }

        public async Task<JobResource> UploadFileResourceAsync(string fileLocalPath, string remoteUploadDirectoryPath)
        {
            var driverUploadPath = remoteUploadDirectoryPath.TrimEnd('/') + @"/";
            var jobArgsFilePath = fileLocalPath;
            return await UploadResourceAndGetInfoAsync(jobArgsFilePath, ResourceType.FILE, driverUploadPath);
        }

        private async Task<JobResource> UploadResourceAndGetInfoAsync(string filePath, ResourceType resourceType, string driverUploadPath, string localizedName = null)
        {
            if (!_file.Exists(filePath))
            {
                Exceptions.Throw(
                    new FileNotFoundException("Could not find resource file " + filePath),
                    Log);
            }

            var detailsOutputPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));

            try
            {
                await _javaLauncher.LaunchAsync(
                    JavaLoggingSetting.Info, 
                    JavaClassNameForResourceUploader,
                    filePath,
                    resourceType.ToString(),
                    driverUploadPath,
                    detailsOutputPath);

                var localizedResourceName = localizedName ?? Path.GetFileName(filePath);
                return ParseGeneratedOutputFile(detailsOutputPath, localizedResourceName, resourceType);
            }
            finally
            {
                if (_file.Exists(detailsOutputPath))
                {
                    _file.Delete(detailsOutputPath);
                }
            }
        }

        private JobResource ParseGeneratedOutputFile(string resourceDetailsOutputPath, string resourceName, ResourceType resourceType)
        {
            if (!_file.Exists(resourceDetailsOutputPath))
            {
                Exceptions.Throw(
                    new FileNotFoundException("Could not find resource details file " + resourceDetailsOutputPath),
                    Log);
            }

            // Single line file, easier to deal with sync read
            string fileContent = _file.ReadAllText(resourceDetailsOutputPath).Trim();

            Log.Log(Level.Info, "Java uploader returned content: " + fileContent);

            _file.Delete(resourceDetailsOutputPath);
            string[] tokens = fileContent.Split(';');

            return new JobResource
            {
                Name = resourceName,
                RemoteUploadPath = tokens[0],
                LastModificationUnixTimestamp = long.Parse(tokens[1]),
                ResourceSize = long.Parse(tokens[2]),
                ResourceType = resourceType
            };
        }
    }
}