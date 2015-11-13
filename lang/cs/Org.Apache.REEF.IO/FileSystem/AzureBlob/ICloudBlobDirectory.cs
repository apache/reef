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

using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Org.Apache.REEF.IO.FileSystem.AzureBlob
{
    /// <summary>
    /// A proxy interface for CloudBlobDirectory, mainly in order to fake for testing.
    /// </summary>
    internal interface ICloudBlobDirectory
    {
        /// <summary>
        /// Gets a reference to a Blob "directory." Note that Azure Blob does not actually support
        /// the concept of directories, so in reality this is more of a convenience method.
        /// </summary>
        /// <param name="directoryName">The name of the blob "directory"</param>
        /// <returns>A reference to the blob "directory"</returns>
        ICloudBlobDirectory GetDirectoryReference(string directoryName);

        /// <summary>
        /// Lists the blobs.
        /// </summary>
        /// <param name="useFlatListing">
        /// If true, recursively lists all blobs.
        /// Otherwise, "directories" can be listed as well.
        /// </param>
        /// <returns>
        /// An <see cref="IEnumerable{T}"/> of blob items that can be either a type of blob
        /// or blob directories.
        /// </returns>
        IEnumerable<IListBlobItem> ListBlobs(bool useFlatListing = false);
    }
}
