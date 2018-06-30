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
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Org.Apache.REEF.IO.FileSystem.AzureBlob.RetryPolicy;

namespace Org.Apache.REEF.IO.FileSystem.AzureBlob
{
    /// <summary>
    /// A proxy class for <see cref="CloudBlockBlob"/>, created mainly
    /// for unit testing purposes.
    /// </summary>
    internal sealed class AzureCloudBlockBlob : ICloudBlockBlob
    {
        private readonly CloudBlockBlob _blob;
        private readonly BlobRequestOptions _requestOptions;

        public ICloudBlob Blob
        {
            get
            {
                return _blob;
            }
        }

        public BlobProperties Properties
        {
            get
            {
                return _blob.Properties;
            }
        }

        public CopyState CopyState
        {
            get
            {
                return _blob.CopyState;
            }
        }

        public AzureCloudBlockBlob(Uri uri, StorageCredentials credentials, BlobRequestOptions requestOptions)
        {
            _blob = new CloudBlockBlob(uri, credentials);
            _requestOptions = requestOptions;
        }

        public Stream Open()
        {
            #if REEF_DOTNET_BUILD
                var task = _blob.OpenReadAsync(null, _requestOptions, null);
                return task.GetAwaiter().GetResult();
            #else
                return _blob.OpenRead(null, _requestOptions, null);
            #endif
        }

        public Stream Create()
        {
            #if REEF_DOTNET_BUILD
                return _blob.OpenWriteAsync(null, _requestOptions, null).GetAwaiter().GetResult();
            #else
                return _blob.OpenWrite(null, _requestOptions, null);
            #endif
        }

        public bool Exists()
        {
            return _blob.ExistsAsync(_requestOptions, null).GetAwaiter().GetResult();
        }

        public void Delete()
        {
            _blob.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots, null, _requestOptions, null).Wait();
        }

        public void DeleteIfExists()
        {
            _blob.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, null, _requestOptions, null).Wait();
        }

        public string StartCopy(Uri source)
        {
            return _blob.StartCopyAsync(source, null, null, _requestOptions, null).GetAwaiter().GetResult();
        }

        public void DownloadToFile(string path, FileMode mode)
        {
            _blob.DownloadToFileAsync(path, mode, null, _requestOptions, null).Wait();
        }

        public void UploadFromFile(string path, FileMode mode)
        {
            _blob.UploadFromFileAsync(path, null, _requestOptions, null).Wait();
        }

        public void FetchAttributes()
        {
            _blob.FetchAttributesAsync(null, _requestOptions, null).Wait();
        }
    }
}