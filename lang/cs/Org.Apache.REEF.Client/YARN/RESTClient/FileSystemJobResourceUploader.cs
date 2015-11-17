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
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.YARN.RestClient
{
    /// <summary>
    /// Provides FileSystem agnostic job resource uploader.
    /// User can provide custome implementation of 
    /// <see cref="IFileSystem"/> for their choice of DFS.
    /// </summary>
    internal sealed class FileSystemJobResourceUploader : IJobResourceUploader
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(LegacyJobResourceUploader));
        private static readonly DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, 0);
        private readonly IJobSubmissionDirectoryProvider _jobSubmissionDirectoryProvider;
        private readonly IResourceArchiveFileGenerator _resourceArchiveFileGenerator;
        private readonly IFileSystem _fileSystem;

        [Inject]
        private FileSystemJobResourceUploader(
            IJobSubmissionDirectoryProvider jobSubmissionDirectoryProvider,
            IResourceArchiveFileGenerator resourceArchiveFileGenerator,
            IFileSystem fileSystem)
        {
            _fileSystem = fileSystem;
            _resourceArchiveFileGenerator = resourceArchiveFileGenerator;
            _jobSubmissionDirectoryProvider = jobSubmissionDirectoryProvider;
        }

        public JobResource UploadJobResource(string driverLocalFolderPath)
        {
            driverLocalFolderPath = driverLocalFolderPath.TrimEnd('\\') + @"\";
            var driverUploadPath = _jobSubmissionDirectoryProvider.GetJobSubmissionRemoteDirectory().TrimEnd('/') + @"/";
            Log.Log(Level.Verbose, "DriverFolderPath: {0} DriverUploadPath: {1}", driverLocalFolderPath, driverUploadPath);
            var archivePath = _resourceArchiveFileGenerator.CreateArchiveToUpload(driverLocalFolderPath);

            var destinationPath = driverUploadPath + Path.GetFileName(archivePath);
            var remoteFileUri = _fileSystem.CreateUriForPath(destinationPath);
            Log.Log(Level.Verbose, @"Copy {0} to {1}", archivePath, remoteFileUri);

            var parentDirectoryUri = _fileSystem.CreateUriForPath(driverUploadPath);
            _fileSystem.CreateDirectory(parentDirectoryUri);
            _fileSystem.CopyFromLocal(archivePath, remoteFileUri);
            var fileStatus = _fileSystem.GetFileStatus(remoteFileUri);

            return new JobResource
            {
                LastModificationUnixTimestamp = DateTimeToUnixTimestamp(fileStatus.ModificationTime),
                RemoteUploadPath = remoteFileUri.AbsoluteUri,
                ResourceSize = fileStatus.LengthBytes
            };
        }

        private long DateTimeToUnixTimestamp(DateTime dateTime)
        {
            return (long) (dateTime - Epoch).TotalSeconds;
        }
    }
}