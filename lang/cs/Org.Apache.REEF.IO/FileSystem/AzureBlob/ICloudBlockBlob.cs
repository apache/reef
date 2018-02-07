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
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Org.Apache.REEF.IO.FileSystem.AzureBlob
{
    /// <summary>
    /// A proxy interface for <see cref="CloudBlockBlob"/>,
    /// mainly used for unit testing purposes.
    /// </summary>
    internal interface ICloudBlockBlob
    {
        /// <summary>
        /// The actual, underlying <see cref="ICloudBlob"/>. Mainly a test hook.
        /// </summary>
        ICloudBlob Blob { get; }

        /// <summary>
        /// The <see cref="BlobProperties"/> of the blob. Note that this metadata
        /// will only be fetched if <see cref="FetchAttributes"/> is called first.
        /// </summary>
        BlobProperties Properties { get; }

        /// <summary>
        /// The <see cref="CopyState"/> of the blob. Note that this metadata
        /// will only be fetched if <see cref="FetchAttributes"/> is called first.
        /// </summary>
        CopyState CopyState { get; }

        /// <summary>
        /// Opens a stream to the blob content.
        /// </summary>
        /// <returns>System.IO.Stream object.</returns>
        /// <exception cref="StorageException">If blob does not exist</exception>
        Stream Open();

        /// <summary>
        /// Creates a blob and returns a write Stream object to it.
        /// </summary>
        /// <returns>System.IO.Stream object.</returns>
        /// <exception cref="StorageException">If blob cannot be created</exception>
        Stream Create();

        /// <summary>
        /// Makes a round trip to the server to test if the blob exists.
        /// </summary>
        /// <returns>True if exists. False otherwise.</returns>
        bool Exists();

        /// <summary>
        /// Deletes the <see cref="ICloudBlockBlob"/> from the server.
        /// </summary>
        /// <exception cref="StorageException">If blob does not exist</exception>
        void Delete();

        /// <summary>
        /// Deletes the <see cref="ICloudBlockBlob"/> from the server, only if it exists.
        /// </summary>
        void DeleteIfExists();

        /// <summary>
        /// Starts the process to copy a <see cref="ICloudBlockBlob"/> to another <see cref="ICloudBlockBlob"/>.
        /// </summary>
        /// <param name="source">The URI of the source <see cref="ICloudBlockBlob"/></param>
        /// <returns>The TaskID of the copy operation</returns>
        string StartCopy(Uri source);

        /// <summary>
        /// Downloads the <see cref="ICloudBlockBlob"/> to a local file.
        /// </summary>
        /// <param name="path">Path to local file</param>
        /// <param name="mode">Mode of the file</param>
        void DownloadToFile(string path, FileMode mode);

        /// <summary>
        /// Uploads to an <see cref="ICloudBlockBlob"/> from a local file.
        /// </summary>
        /// <param name="path">Path to local file</param>
        /// <param name="mode">Mode of the file</param>
        void UploadFromFile(string path, FileMode mode);

        /// <summary>
        /// Makes a round trip to the server to fetch the metadata of the <see cref="ICloudBlockBlob"/>.
        /// </summary>
        void FetchAttributes();
    }
}
