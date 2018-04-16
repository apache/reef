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

using Microsoft.WindowsAzure.Storage.Blob;

namespace Org.Apache.REEF.IO.FileSystem.AzureBlob
{
    /// <summary>
    /// A proxy class for CloudBlobContainer, mainly in order to fake for unit testing.
    /// </summary>
    internal sealed class AzureCloudBlobContainer : ICloudBlobContainer
    {
        private readonly CloudBlobContainer _container;

        public AzureCloudBlobContainer(CloudBlobContainer container)
        {
            _container = container;
        }

        public bool CreateIfNotExists()
        {
            return _container.CreateIfNotExists();
        }

        public void DeleteIfExists()
        {
            _container.DeleteIfExistsAsync().Wait();
        }

        public ICloudBlobDirectory GetDirectoryReference(string directoryName)
        {
            return new AzureCloudBlobDirectory(_container.GetDirectoryReference(directoryName));
        }
    }
}
