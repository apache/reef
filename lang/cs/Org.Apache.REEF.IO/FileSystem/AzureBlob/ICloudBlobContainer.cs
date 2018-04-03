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

namespace Org.Apache.REEF.IO.FileSystem.AzureBlob
{
    /// <summary>
    /// A proxy interface for CloudBlobContainer, mainly in order to fake for testing.
    /// </summary>
    internal interface ICloudBlobContainer
    {
        /// <summary>
        /// Creates the <see cref="ICloudBlobContainer"/> if it does not exist already.
        /// </summary>
        /// <returns>Whether a new container was created or not</returns>
        bool CreateIfNotExists();

        /// <summary>
        /// Deletes the <see cref="ICloudBlobContainer"/> if it exists.
        /// Does a round trip to the Blob Server.
        /// </summary>
        void DeleteIfExists();

        /// <summary>
        /// Gets a reference to a blob "directory." Note that Azure Blob does not actually support
        /// the concept of directories, so in reality this is more of a convenience method.
        /// </summary>
        /// <param name="directoryName">Name of the "directory"</param>
        /// <returns>The reference to a "directory"</returns>
        ICloudBlobDirectory GetDirectoryReference(string directoryName);
    }
}
