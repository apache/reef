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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Azure.DataLake.Store;
using Microsoft.Azure.DataLake.Store.FileTransfer;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IO.FileSystem.AzureDataLake
{
    /// <summary>
    /// An IFileSystem implementation for Azure Data Lake Store.
    /// </summary>
    internal sealed class AzureDataLakeFileSystem : IFileSystem
    {
        private readonly IDataLakeStoreClient _client;
        private readonly AdlsClient _adlsClient;

        private string UriPrefix
        {
            get { return $"adl://{_client.AccountFQDN}"; }
        }

        [Inject]
        private AzureDataLakeFileSystem(IDataLakeStoreClient client)
        {
            _client = client;
            _adlsClient = _client.GetAdlsClient();
        }

        /// <summary>
        /// Opens the given URI for reading
        /// </summary>
        /// <exception cref="AdlsException">If the URI couldn't be opened.</exception>
        public Stream Open(Uri fileUri)
        {
            return _adlsClient.GetReadStream(fileUri.AbsolutePath);
        }

        /// <summary>
        /// Creates a new file under the given URI.
        /// </summary>
        /// <exception cref="AdlsException">If the URI couldn't be created.</exception>
        public Stream Create(Uri fileUri)
        {
            return _adlsClient.CreateFile(fileUri.AbsolutePath, IfExists.Overwrite);
        }

        /// <summary>
        /// Deletes the file under the given URI.
        /// </summary>
        /// <exception cref="IOException">If the specified file cannot be deleted</exception>
        public void Delete(Uri fileUri)
        {
            bool deleteStatus = _adlsClient.Delete(fileUri.AbsolutePath);
            if (!deleteStatus)
            {
                throw new IOException($"Cannot delete directory/file specified by {fileUri}");
            }
        }

        /// <summary>
        /// Determines whether a file exists under the given URI.
        /// </summary>
        public bool Exists(Uri fileUri)
        {
            return _adlsClient.CheckExists(fileUri.AbsolutePath);
        }

        /// <summary>
        /// Copies the file referenced by sourceUri to destinationUri.
        /// Note : This method reads from the input stream of sourceUri locally and
        /// writes to the output stream of destinationUri.
        /// This is time consuming and not recommended for large file transfers.
        /// </summary>
        /// <exception cref="IOException">If copy process encounters any exceptions</exception>
        public void Copy(Uri sourceUri, Uri destinationUri)
        {
            try
            {
                using (var readStream = Open(sourceUri))
                {
                    readStream.Position = 0;
                    using (var writeStream = Create(destinationUri))
                    {
                        readStream.CopyTo(writeStream);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new IOException($"Error copying {sourceUri} to {destinationUri}", ex);
            }
        }

        /// <summary>
        /// Copies the remote file to a local file.
        /// </summary>
        /// <exception cref="IOException">If copy process encounters any exceptions</exception>
        public void CopyToLocal(Uri remoteFileUri, string localFileName)
        {
            TransferStatus status;
            try
            {
                status = _adlsClient.BulkDownload(remoteFileUri.AbsolutePath, localFileName); // throws KeyNotFoundException
            }
            catch (Exception ex)
            {
                throw new IOException($"Error in bulk download from {remoteFileUri} to {localFileName}", ex);
            }
            if (status.EntriesFailed.Count != 0)
            {
                throw new IOException($"{status.EntriesFailed.Count} entries did not get transferred correctly");
            }
        }

        /// <summary>
        /// Copies the specified file to the remote location.
        /// </summary>
        /// <exception cref="IOException">If copy process encounters any exception</exception>
        public void CopyFromLocal(string localFileName, Uri remoteFileUri)
        {
            TransferStatus status;
            try
            {
                status = status = _adlsClient.BulkUpload(localFileName, remoteFileUri.AbsolutePath);
            }
            catch (Exception ex)
            {
                throw new IOException($"Error in bulk upload from {localFileName} to {remoteFileUri}", ex);
            }
            if (status.EntriesFailed.Count != 0)
            {
                throw new IOException($"{status.EntriesFailed.Count} entries did not get transferred correctly");
            }
        }

        /// <summary>
        /// Creates a new directory.
        /// </summary>
        /// <exception cref="IOException">If directory cannot be created</exception>
        public void CreateDirectory(Uri directoryUri)
        {
            bool createDirStatus = _adlsClient.CreateDirectory(directoryUri.AbsolutePath);
            if (!createDirStatus)
            {
                throw new IOException($"Cannot create directory specified by {directoryUri}");
            }
        }

        /// <summary>
        /// Deletes a directory.
        /// </summary>
        /// <exception cref="IOException">If directory cannot be deleted</exception>
        public void DeleteDirectory(Uri directoryUri)
        {
            bool deleteStatus = Exists(directoryUri) && 
                _adlsClient.GetDirectoryEntry(directoryUri.AbsolutePath).Type == DirectoryEntryType.DIRECTORY &&
                _adlsClient.DeleteRecursive(directoryUri.AbsolutePath);
            if (!deleteStatus)
            {
                throw new IOException($"Cannot delete directory specified by {directoryUri}");
            }
        }

        /// <summary>
        /// Get the children on the given URI, if that refers to a directory.
        /// </summary>
        /// <exception cref="IOException">If directory does not exist</exception>
        public IEnumerable<Uri> GetChildren(Uri directoryUri)
        {
            if (!Exists(directoryUri) || _adlsClient.GetDirectoryEntry(directoryUri.AbsolutePath).Type != DirectoryEntryType.DIRECTORY)
            {
                throw new IOException($"Cannot find directory specified by {directoryUri}");
            }

            return _adlsClient.EnumerateDirectory(directoryUri.AbsolutePath).Select(entry => CreateUriForPath(entry.FullName));
        }

        /// <summary>
        /// Create Uri from a given file path.
        /// The file path can be full with prefix or relative without prefix.
        /// If path is null or the prefix doesn't match the prefix in the FileSystem, throw ArgumentException.
        /// </summary>
        /// <exception cref="ArgumentNullException">If specified path is null</exception>
        public Uri CreateUriForPath(string path)
        {
            if (path == null)
            {
                throw new ArgumentNullException(nameof(path), "Specified path is null");
            }

            Uri resultUri = null;

            try
            {
                resultUri = new Uri(path);
            }
            catch (UriFormatException)
            {
                resultUri = new Uri(new Uri(this.UriPrefix), path);
            }

            if (!resultUri.AbsoluteUri.StartsWith(this.UriPrefix))
            {
                throw new ArgumentException($"Given uri must begin with valid prefix ({this.UriPrefix})", nameof(path));
            }

            return resultUri;
        }

        /// <summary>
        /// Gets the FileStatus for remote file.
        /// </summary>
        /// <exception cref="ArgumentNullException">If remote file URI is null</exception>
        /// <returns>FileStatus</returns>
        public FileStatus GetFileStatus(Uri remoteFileUri)
        {
            if (remoteFileUri == null)
            {
                throw new ArgumentNullException(nameof(remoteFileUri), "Specified uri is null");
            }
            var entrySummary = _adlsClient.GetDirectoryEntry(remoteFileUri.AbsolutePath);
            if (!entrySummary.LastModifiedTime.HasValue)
            {
                throw new IOException($"File/Directory at {remoteFileUri} does not have a last modified time. It may have been deleted.");
            }

            return new FileStatus(entrySummary.LastModifiedTime.Value, entrySummary.Length);
        }
    }
}
