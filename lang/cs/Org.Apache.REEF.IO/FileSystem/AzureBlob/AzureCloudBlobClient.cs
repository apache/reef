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
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Org.Apache.REEF.IO.FileSystem.AzureBlob
{
    /// <summary>
    /// A proxy class for CloudBlobClient, mainly in order to fake for testing.
    /// </summary>
    internal sealed class AzureCloudBlobClient : ICloudBlobClient
    {
        private readonly CloudBlobClient _client;

        public StorageCredentials Credentials { get { return _client.Credentials; } }

        public AzureCloudBlobClient(Uri baseUri)
        {
            _client = new CloudBlobClient(baseUri);
        }

        public AzureCloudBlobClient(Uri baseUri, StorageCredentials storageCredentials)
        {
            _client = new CloudBlobClient(baseUri, storageCredentials);
        }

        public AzureCloudBlobClient(StorageUri storageUri, StorageCredentials storageCredentials)
        {
            _client = new CloudBlobClient(storageUri, storageCredentials);
        }

        public AzureCloudBlobClient(string connectionString)
        {
            _client = CloudStorageAccount.Parse(connectionString).CreateCloudBlobClient();
        }

        public Uri BaseUri
        {
            get { return _client.BaseUri; }
        }

        public ICloudBlob GetBlobReferenceFromServer(Uri blobUri)
        {
            return _client.GetBlobReferenceFromServer(blobUri);
        }

        public ICloudBlobContainer GetContainerReference(string containerName)
        {
            return new AzureCloudBlobContainer(_client.GetContainerReference(containerName));
        }

        public BlobResultSegment ListBlobsSegmented(string prefix, bool useFlatListing, BlobListingDetails blobListingDetails,
            int? maxResults, BlobContinuationToken continuationToken, BlobRequestOptions blobRequestOptions,
            OperationContext operationContext)
        {
            return _client.ListBlobsSegmented(prefix, useFlatListing, blobListingDetails, maxResults, continuationToken,
                blobRequestOptions, operationContext);
        }
    }
}
