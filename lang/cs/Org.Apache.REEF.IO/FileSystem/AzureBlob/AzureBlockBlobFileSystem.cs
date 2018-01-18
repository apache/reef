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
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IO.FileSystem.AzureBlob
{
    /// <summary>
    /// An IFileSystem implementation for Azure Blob storage.
    /// This particular File System implementation only supports 
    /// block blob operations
    /// </summary>
    internal sealed class AzureBlockBlobFileSystem : IFileSystem
    {
        private readonly ICloudBlobClient _client;

        [Inject]
        private AzureBlockBlobFileSystem(ICloudBlobClient client)
        {
            _client = client;
        }

        /// <summary>
        /// Returns a Stream object to the blob specified by the fileUri.
        /// </summary>
        public Stream Open(Uri fileUri)
        {
            return _client.GetBlockBlobReference(fileUri).Open();
        }

        /// <summary>
        /// Creates a blob for the specified fileUri and returns a write Stream object to it.
        /// </summary>
        public Stream Create(Uri fileUri)
        {
            return _client.GetBlockBlobReference(fileUri).Create();
        }

        /// <summary>
        /// Deletes a CloudBlockBlob
        /// <see cref="IFileSystem.Delete"/>
        /// </summary>
        public void Delete(Uri fileUri)
        {
            _client.GetBlockBlobReference(fileUri).Delete();
        }

        /// <summary>
        /// Checks if a CloudBlockBlob exists.
        /// <see cref="IFileSystem.Exists"/>
        /// </summary>
        public bool Exists(Uri fileUri)
        {
            return _client.GetBlockBlobReference(fileUri).Exists();
        }

        /// <summary>
        /// Copies to a CloudBlockBlob from another CloudBlockBlob.
        /// <see cref="IFileSystem.Copy"/>
        /// </summary>
        public void Copy(Uri sourceUri, Uri destinationUri)
        {
            _client.GetBlockBlobReference(destinationUri).StartCopy(sourceUri);
            var blockBlob = _client.GetBlockBlobReference(destinationUri);
            blockBlob.FetchAttributes();

            while (blockBlob.CopyState.Status == CopyStatus.Pending)
            {
                Task.Delay(TimeSpan.FromMilliseconds(50));
                blockBlob.FetchAttributes();
            }
        }

        /// <summary>
        /// Copies from a CloudBlockBlob to a local file.
        /// <see cref="IFileSystem.CopyToLocal"/>
        /// </summary>
        public void CopyToLocal(Uri remoteFileUri, string localName)
        {
            _client.GetBlockBlobReference(remoteFileUri).DownloadToFile(localName, FileMode.CreateNew);
        }

        /// <summary>
        // Copies to a CloudBlockBlob from a local file.
        /// <see cref="IFileSystem.CopyFromLocal"/>
        /// </summary>
        public void CopyFromLocal(string localFileName, Uri remoteFileUri)
        {
            _client.GetBlockBlobReference(remoteFileUri).UploadFromFile(localFileName, FileMode.Open);
        }

        /// <summary>
        /// Azure blobs does not have the sense of the existence of a "directory."
        /// We will not throw an error, but we will not do anything either.
        /// </summary>
        public void CreateDirectory(Uri directoryUri)
        {
        }

        /// <summary>
        /// Recursively deletes blobs under a specified "directory URI."
        /// If only the container is specified, the entire container is deleted.
        /// </summary>
        public void DeleteDirectory(Uri directoryUri)
        {
            var uriSplit = directoryUri.AbsolutePath.Split(new[] { "/" }, StringSplitOptions.RemoveEmptyEntries);
            if (!uriSplit.Any())
            {
                throw new StorageException(string.Format("URI {0} must contain at least the container.", directoryUri));
            }

            var containerName = uriSplit[0];
            var container = _client.GetContainerReference(containerName);
            if (uriSplit.Length == 1)
            {
                container.DeleteIfExists();
                return;
            }

            var directory = container.GetDirectoryReference(uriSplit[1]);

            for (var i = 2; i < uriSplit.Length; i++)
            {
                directory = directory.GetDirectoryReference(uriSplit[i]);
            }

            foreach (var blob in directory.ListBlobs(true).OfType<ICloudBlob>())
            {
                blob.DeleteIfExistsAsync().Wait();
            }
        }

        /// <summary>
        /// Gets the children of the blob "directory."
        /// </summary>
        public IEnumerable<Uri> GetChildren(Uri directoryUri)
        {
            BlobContinuationToken blobContinuationToken = null;
            var path = directoryUri.AbsolutePath.Trim('/');

            do
            {
                var listing = _client.ListBlobsSegmented(path, false,
                    BlobListingDetails.None, null,
                    blobContinuationToken, new BlobRequestOptions(), new OperationContext());

                if (listing.Results != null)
                {
                    foreach (var listBlobItem in listing.Results)
                    {
                        yield return listBlobItem.Uri;
                    }
                }

                blobContinuationToken = listing.ContinuationToken;
            } 
            while (blobContinuationToken != null);
        }

        /// <summary>
        /// Creates a Uri using the relative path to the remote file (including the container),
        /// getting the absolute URI from the Blob client's base URI.
        /// </summary>
        /// <param name="path">The relative path to the remote file, including the container</param>
        /// <returns>The URI to the remote file</returns>
        public Uri CreateUriForPath(string path)
        {
            return new Uri(_client.BaseUri.AbsoluteUri.TrimEnd('/') + '/' + path.Trim('/'));
        }

        /// <summary>
        /// Gets the FileStatus based on reports from Blob.
        /// </summary>
        public FileStatus GetFileStatus(Uri remoteFileUri)
        {
            var blobReference = _client.GetBlockBlobReference(remoteFileUri);
            if (!blobReference.Exists())
            {
                throw new StorageException("Unable to find blob at " + remoteFileUri);
            }

            blobReference.FetchAttributes();

            var lastModifiedTime = blobReference.Properties.LastModified;
            if (!lastModifiedTime.HasValue)
            {
                throw new StorageException("Blob at " + remoteFileUri + " does not have a last modified" +
                                           "time. It may have been deleted.");
            }

            return new FileStatus(lastModifiedTime.Value.DateTime, blobReference.Properties.Length);
        }
    }
}
