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
using Microsoft.Rest;
using Microsoft.Rest.Azure.Authentication;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;
using System.Threading;
using Org.Apache.REEF.IO.FileSystem.AzureDataLake;
using Microsoft.Azure.DataLake.Store;
using Org.Apache.REEF.Tang.Interface;
using System.IO;
using System.Linq;

namespace Org.Apache.REEF.IO.Tests
{
    /// <summary>
    /// E2E tests for AzureDataLakeFileSystem.
    /// These tests require the person running the test to fill in credentials.
    /// </summary>
    public sealed class TestAzureDataLakeFileSystemE2E : IDisposable
    {
        private const string SkipMessage = "Fill in credentials before running test"; // Use null to run tests
        private const string ContentsText = "hello";
        private readonly IFileSystem _fileSystem;
        private readonly AdlsClient _adlsClient;
        private readonly string _defaultFolderName;

        public TestAzureDataLakeFileSystemE2E()
        {
            // Service principal / application authentication with client secret / key
            // Use the application ID of an existing AAD "Web App" application as the ClientId and
            // use its authentication key as the SecretKey
            // https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-create-service-principal-portal#get-application-id-and-authentication-key
            // Fill in before running test!
            const string AdlsAccountFqdn = "#####.azuredatalakestore.net";
            const string Tenant = "microsoft.onmicrosoft.com";
            const string TokenAudience = @"https://datalake.azure.net/";
            const string ClientId = "########-####-####-####-############"; // e.g. "c2897d56-5eef-4030-8b7a-46b5c0acd05c"
            const string SecretKey = "##########"; // e.g. "SecretKey1234!"

            _defaultFolderName = "reef-test-folder-" + Guid.NewGuid();

            IConfiguration conf = AzureDataLakeFileSystemConfiguration.ConfigurationModule
                .Set(AzureDataLakeFileSystemConfiguration.DataLakeStorageAccountFqdn, AdlsAccountFqdn)
                .Set(AzureDataLakeFileSystemConfiguration.Tenant, Tenant)
                .Set(AzureDataLakeFileSystemConfiguration.ClientId, ClientId)
                .Set(AzureDataLakeFileSystemConfiguration.SecretKey, SecretKey)
                .Build();

            _fileSystem = TangFactory.GetTang().NewInjector(conf).GetInstance<AzureDataLakeFileSystem>();

            ServiceClientCredentials adlCreds = GetCredsSpiSecretKey(Tenant, new Uri(TokenAudience), ClientId, SecretKey);
            _adlsClient = AdlsClient.CreateClient(AdlsAccountFqdn, adlCreds);
        }

        public void Dispose()
        {
            _adlsClient?.DeleteRecursive($"/{_defaultFolderName}");
        }

        [Fact(Skip = SkipMessage)]
        public void TestOpenE2E()
        {
            string fileName = UploadFromString(ContentsText);
            using (var reader = new StreamReader(_fileSystem.Open(PathToFile(fileName))))
            {
                string streamText = reader.ReadToEnd();
                Assert.Equal(ContentsText, streamText);
            }
        }

        [Fact(Skip = SkipMessage)]
        public void TestCreateE2E()
        {
            string fileName = $"/{_defaultFolderName}/TestCreateE2E.txt";
            var stream = _fileSystem.Create(PathToFile(fileName));
            Assert.True(_adlsClient.CheckExists(fileName));
            Assert.IsType<AdlsOutputStream>(stream);
        }

        [Fact(Skip = SkipMessage)]
        public void TestDeleteE2E()
        {
            string fileName = UploadFromString(ContentsText);
            Assert.True(_adlsClient.CheckExists(fileName));
            _fileSystem.Delete(PathToFile(fileName));
            Assert.False(_adlsClient.CheckExists(fileName));
        }

        [Fact(Skip = SkipMessage)]
        public void TestDeleteExceptionE2E()
        {
            Assert.Throws<IOException>(() => _fileSystem.Delete(PathToFile("fileName")));
        }

        [Fact(Skip = SkipMessage)]
        public void TestExistsE2E()
        {
            string fileName = UploadFromString(ContentsText);
            Assert.True(_fileSystem.Exists(PathToFile(fileName)));
            _adlsClient.Delete(fileName);
            Assert.False(_fileSystem.Exists(PathToFile(fileName)));
        }

        [Fact(Skip = SkipMessage)]
        public void TestCopyE2E()
        {
            var sourceTempFilePath = Path.GetTempFileName();
            var destTempFilePath = Path.GetTempFileName();
            try
            {
                string fileName = UploadFromString("CopyThis", 1);
                var sourceUri = PathToFile(fileName);
                string copyToFile = $"/{_defaultFolderName}/testFile2.txt";
                var destUri = PathToFile(copyToFile);
                _fileSystem.Copy(sourceUri, destUri);
                _adlsClient.BulkDownload(sourceUri.AbsolutePath, sourceTempFilePath);
                _adlsClient.BulkDownload(destUri.AbsolutePath, destTempFilePath);
                FileSystemTestUtilities.HaveSameContent(sourceTempFilePath, destTempFilePath);
            }
            finally
            {
                try
                {
                    File.Delete(sourceTempFilePath);
                }
                finally
                {
                    File.Delete(destTempFilePath);
                }
            }
        }

        [Fact(Skip = SkipMessage)]
        public void TestCopyToLocalE2E()
        {         
            var tempFilePath = Path.GetTempFileName();
            try
            {
                string fileName = UploadFromString(ContentsText);
                _fileSystem.CopyToLocal(PathToFile(fileName), tempFilePath);
                Assert.True(File.Exists(tempFilePath));
                Assert.Equal(ContentsText, File.ReadAllText(tempFilePath));
            }
            finally
            {
                File.Delete(tempFilePath);
            }
        }

        [Fact(Skip = SkipMessage)]
        public void TestCopyFromLocalE2E()
        {
            var tempFilePath = Path.GetTempFileName();
            var tempFileName = Path.GetFileName(tempFilePath);
            try
            {
                File.WriteAllText(tempFilePath, ContentsText);
                Uri remoteFileUri = PathToFile($"/{_defaultFolderName}/{tempFileName}");
                _fileSystem.CopyFromLocal(tempFilePath, remoteFileUri);
                Assert.True(_adlsClient.CheckExists($"/{_defaultFolderName}/{tempFileName}"));
                var stream = _fileSystem.Open(remoteFileUri);
                string streamText = new StreamReader(stream).ReadToEnd();
                Assert.Equal(ContentsText, streamText);
            }
            finally
            {
                File.Delete(tempFilePath);
            }
        }

        [Fact(Skip = SkipMessage)]
        public void TestCreateDirectoryE2E()
        {
            string dirName = $"/{_defaultFolderName}";
            _fileSystem.CreateDirectory(PathToFile(dirName));
            Assert.True(_adlsClient.CheckExists(dirName));
        }

        [Fact(Skip = SkipMessage)]
        public void TestIsDirectoryE2E()
        {
            string dirName = $"/{_defaultFolderName}";
            string fakeDirName = $"/fakeDir";

            _adlsClient.CreateDirectory(dirName);
            string fileName = UploadFromString(ContentsText);

            Assert.True(_fileSystem.IsDirectory(PathToFile(dirName)));
            Assert.False(_fileSystem.IsDirectory(PathToFile(fakeDirName)));
            Assert.False(_fileSystem.IsDirectory(PathToFile(fileName)));
        }

        [Fact(Skip = SkipMessage)]
        public void TestDeleteDirectoryE2E()
        {
            string dirName = $"/{_defaultFolderName}";
            _adlsClient.CreateDirectory(dirName);
            Assert.True(_adlsClient.CheckExists(dirName));
            _fileSystem.Delete(PathToFile(dirName));
            Assert.False(_adlsClient.CheckExists(dirName));
        }

        [Fact(Skip = SkipMessage)]
        public void TestGetChildrenE2E()
        {
            string fileName1 = UploadFromString("file1", 1);
            string fileName2 = UploadFromString("file2", 2);
            string dirName = $"/{_defaultFolderName}";
            var childUris = _fileSystem.GetChildren(PathToFile(dirName)).ToList();
            Assert.Equal(2, childUris.Count);
            Assert.Equal(new[] { PathToFile(fileName1), PathToFile(fileName2) }, childUris);
        }

        [Fact(Skip = SkipMessage)]
        public void TestGetFileStatusE2E()
        {
            string fileName = UploadFromString(ContentsText);
            var fileStatus = _fileSystem.GetFileStatus(PathToFile(fileName));
            Assert.Equal(ContentsText.Length, fileStatus.LengthBytes);
        }

        private static ServiceClientCredentials GetCredsSpiSecretKey(string tenant, Uri tokenAudience, string clientId, string secretKey)
        {
            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());

            var serviceSettings = ActiveDirectoryServiceSettings.Azure;
            serviceSettings.TokenAudience = tokenAudience;

            return ApplicationTokenProvider.LoginSilentAsync(tenant, clientId, secretKey, serviceSettings).Result;
        }

        private string UploadFromString(string str, int fileIndex = 1)
        {
            string fileName = $"/{_defaultFolderName}/testFile{fileIndex}.txt";
            using (var streamWriter = new StreamWriter(_adlsClient.CreateFile(fileName, IfExists.Overwrite)))
            {
                streamWriter.Write(str);
            }
            return fileName;
        }

        private Uri PathToFile(string filePath)
        {
            return _fileSystem.CreateUriForPath(filePath);
        }
    }
}
