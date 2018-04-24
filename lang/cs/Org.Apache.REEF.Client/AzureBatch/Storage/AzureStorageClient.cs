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

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Org.Apache.REEF.Client.AzureBatch.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Client.AzureBatch.Storage
{
    internal sealed class AzureStorageClient
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(AzureStorageClient));
        private const string StorageConnectionStringFormat = "DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}";
        private const int SASTokenValidityMinutes = 60;

        private readonly string _storageAccountName;
        private readonly string _storageAccountKey;
        private readonly string _storageContainerName;

        private readonly string _storageConnectionString;

        [Inject]
        AzureStorageClient(
            [Parameter(typeof(AzureStorageAccountName))] string storageAccountName,
            [Parameter(typeof(AzureStorageAccountKey))] string storageAccountKey,
            [Parameter(typeof(AzureStorageContainerName))] string storageContainerName)
        {
            this._storageAccountName = storageAccountName;
            this._storageAccountKey = storageAccountKey;
            this._storageContainerName = storageContainerName;

            this._storageConnectionString = string.Format(StorageConnectionStringFormat,
                new object[] { storageAccountName, storageAccountKey });
        }

        /// <summary>
        /// Uploads a given file to the given destination folder in Azure Storage.
        /// </summary>
        /// <param name="destination">Destination in Azure Storage where given file will be uploaded.</param>
        /// <param name="filePath">Path to the file to be uploaded.</param>
        /// <returns>Storage SAS URI for uploaded file.</returns>
        public async Task<Uri> UploadFile(string destination, string filePath)
        {
            CloudBlobContainer blobContainer = await this.GetOrCreateCloudBlobContainer();
            CloudBlobDirectory directory = blobContainer.GetDirectoryReference(destination);

            string fileName = Path.GetFileName(filePath);
            CloudBlockBlob blob = directory.GetBlockBlobReference(fileName);
            await blob.UploadFromFileAsync(filePath);

            string sas = blob.GetSharedAccessSignature(CreateSASPolicy());
            string uri = blob.Uri.AbsoluteUri;
            Uri uploadedFile = new Uri(uri + sas);
            LOGGER.Log(Level.Info, "Uploaded {0} jar file to {1}", new string[] { filePath, uploadedFile.ToString() });
            return uploadedFile;
        }

        public string CreateContainerSharedAccessSignature()
        {
            CloudBlobClient cloudBlobClient = CloudStorageAccount.Parse(this._storageConnectionString).CreateCloudBlobClient();
            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference(this._storageContainerName);
            cloudBlobContainer.CreateIfNotExists();
            return cloudBlobContainer.GetSharedAccessSignature(CreateSASPolicy());
        }

        private CloudBlobClient GetCloudBlobClient()
        {
            return CloudStorageAccount.Parse(this._storageConnectionString).CreateCloudBlobClient();
        }

        private async Task<CloudBlobContainer> GetOrCreateCloudBlobContainer()
        {
            CloudBlobClient blobClient = this.GetCloudBlobClient();
            CloudBlobContainer blobContainer = blobClient.GetContainerReference(this._storageContainerName);
            await blobContainer.CreateIfNotExistsAsync();

            return blobContainer;
        }

        private SharedAccessBlobPolicy CreateSASPolicy()
        {
            return new SharedAccessBlobPolicy()
            {
                SharedAccessStartTime = DateTime.UtcNow,
                SharedAccessExpiryTime = DateTime.UtcNow.AddMinutes(SASTokenValidityMinutes),
                Permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write
            };
        }
    }
}
