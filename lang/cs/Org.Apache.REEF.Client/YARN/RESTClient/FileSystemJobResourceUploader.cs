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
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.YARN.RestClient
{
    /// <summary>
    /// Provides FileSystem agnostic job resource uploader.
    /// User can provide custom implementation of
    /// <see cref="IFileSystem"/> for their choice of DFS.
    /// </summary>
    internal sealed class FileSystemJobResourceUploader : IJobResourceUploader
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(FileSystemJobResourceUploader));
        private static readonly DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, 0);
        private readonly IResourceArchiveFileGenerator _resourceArchiveFileGenerator;
        private readonly IFileSystem _fileSystem;
        private readonly IFile _file;
        private readonly REEFFileNames _reefFileNames;
        private readonly IResourceFileRemoteUrlToClusterUrlConverter _urlConverter;

        [Inject]
        private FileSystemJobResourceUploader(
            IResourceArchiveFileGenerator resourceArchiveFileGenerator,
            IFileSystem fileSystem,
            REEFFileNames reefFileNames,
            IFile file,
            IResourceFileRemoteUrlToClusterUrlConverter urlConverter)
        {
            _urlConverter = urlConverter;
            _reefFileNames = reefFileNames;
            _fileSystem = fileSystem;
            _resourceArchiveFileGenerator = resourceArchiveFileGenerator;
            _file = file;
        }

        public async Task<JobResource> UploadArchiveResourceAsync(string driverLocalFolderPath, string remoteUploadDirectoryPath)
        {
            driverLocalFolderPath = driverLocalFolderPath.TrimEnd('\\') + @"\";
            var driverUploadPath = remoteUploadDirectoryPath.TrimEnd('/') + @"/";
            var parentDirectoryUri = _fileSystem.CreateUriForPath(remoteUploadDirectoryPath);
            Log.Log(Level.Verbose, "DriverFolderPath: {0} DriverUploadPath: {1}", driverLocalFolderPath, driverUploadPath);
            
            _fileSystem.CreateDirectory(parentDirectoryUri);

            var archivePath = _resourceArchiveFileGenerator.CreateArchiveToUpload(driverLocalFolderPath);
            return await UploadResourceAndGetInfoAsync(archivePath, ResourceType.ARCHIVE, driverUploadPath, _reefFileNames.GetReefFolderName());
        }

        public async Task<JobResource> UploadFileResourceAsync(string fileLocalPath, string remoteUploadDirectoryPath)
        {
            var driverUploadPath = remoteUploadDirectoryPath.TrimEnd('/') + @"/";
            var parentDirectoryUri = _fileSystem.CreateUriForPath(driverUploadPath);

            _fileSystem.CreateDirectory(parentDirectoryUri);
            return await UploadResourceAndGetInfoAsync(fileLocalPath, ResourceType.FILE, remoteUploadDirectoryPath);
        }

        private async Task<JobResource> UploadResourceAndGetInfoAsync(string filePath, ResourceType resourceType, string driverUploadPath, string localizedName = null)
        {
            if (!_file.Exists(filePath))
            {
                throw new FileNotFoundException("Could not find resource file " + filePath);
            }

            var destinationPath = driverUploadPath + Path.GetFileName(filePath);
            var remoteFileUri = _fileSystem.CreateUriForPath(destinationPath);

            Log.Log(Level.Verbose, @"Copy {0} to {1}", filePath, remoteFileUri);

            // IFileSystem does not support async APIs; Hence we start the copying in a task
            await Task.Run(() => _fileSystem.CopyFromLocal(filePath, remoteFileUri));
            var fileStatus = _fileSystem.GetFileStatus(remoteFileUri);

            return new JobResource
            {
                Name = localizedName ?? Path.GetFileName(filePath),
                LastModificationUnixTimestamp = DateTimeToUnixTimestamp(fileStatus.ModificationTime),
                RemoteUploadPath = _urlConverter.ConvertToLocalUrl(remoteFileUri),
                ResourceSize = fileStatus.LengthBytes,
                ResourceType = resourceType
            };
        }

        private static long DateTimeToUnixTimestamp(DateTime dateTime)
        {
            return (long)(dateTime - Epoch).TotalMilliseconds;
        }
    }
}