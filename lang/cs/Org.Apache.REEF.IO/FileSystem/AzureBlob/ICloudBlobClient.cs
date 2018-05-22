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
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IO.FileSystem.AzureBlob
{
    /// <summary>
    /// A proxy interface for CloudBlobClient, mainly in order to fake for unit testing.
    /// </summary>
    [DefaultImplementation(typeof(AzureCloudBlobClient))]
    internal interface ICloudBlobClient
    {
        /// <summary>
        /// Returns the StorageCredentials used to instantiate the <see cref="ICloudBlobClient"/>.
        /// </summary>
        StorageCredentials Credentials { get; }

        /// <summary>
        /// Returns the Base URI for the <see cref="ICloudBlobClient"/>.
        /// </summary>
        Uri BaseUri { get; }

        /// <summary>
        /// Gets a container reference for a storage account.
        /// </summary>
        /// <param name="containerName">Name of the container</param>
        /// <returns>A reference to the blob container</returns>
        ICloudBlobContainer GetContainerReference(string containerName);

        /// <summary>
        /// Gets a reference to a block blob. Note that the metadata of the blob
        /// will not be filled in, since this method does not do a round trip
        /// to the server.
        /// </summary>
        /// <param name="uri">The absolute URI to the block blob</param>
        /// <returns>A reference to the block blob</returns>
        ICloudBlockBlob GetBlockBlobReference(Uri uri);

        /// <summary>
        /// Paginates a blob listing with container and directory.
        /// </summary>
        BlobResultSegment ListBlobsSegmented(string containerName, string directoryName, bool useFlatListing, BlobListingDetails blobListingDetails, int? maxResults, 
            BlobContinuationToken continuationToken, BlobRequestOptions blobRequestOptions, OperationContext operationContext);
    }
}
