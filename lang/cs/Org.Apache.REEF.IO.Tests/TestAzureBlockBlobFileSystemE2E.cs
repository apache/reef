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
using System.Text;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.FileSystem.AzureBlob;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.IO.Tests
{
    /// <summary>
    /// E2E tests for AzureBlockBlobFileSystem.
    /// These tests require the person running the test to fill in credentials.
    /// </summary>
    public sealed class TestAzureBlockBlobFileSystemE2E : IDisposable
    {
        private const string SkipMessage = "Fill in credentials before running test"; // Use null to run tests
        private const string HelloFile = "hello";
        private IFileSystem _fileSystem;
        private CloudBlobContainer _container;

        public TestAzureBlockBlobFileSystemE2E()
        {
            // Fill in before running test!
            const string ConnectionString = "DefaultEndpointsProtocol=http;AccountName=myAccount;AccountKey=myKey;";
            var defaultContainerName = "reef-test-container-" + Guid.NewGuid();
            var conf = AzureBlockBlobFileSystemConfiguration.ConfigurationModule
                .Set(AzureBlockBlobFileSystemConfiguration.ConnectionString, ConnectionString)
                .Build();

            _fileSystem = TangFactory.GetTang().NewInjector(conf).GetInstance<AzureBlockBlobFileSystem>();
            _container = CloudStorageAccount.Parse(ConnectionString).CreateCloudBlobClient().GetContainerReference(defaultContainerName);
            _container.CreateIfNotExistsAsync().Wait();
        }

        public void Dispose()
        {
            if (_container != null)
            {
                _container.DeleteIfExistsAsync().Wait();
            }
        }

        private bool CheckBlobExists(ICloudBlob blob)
        {
            var task = blob.ExistsAsync();
            task.Wait();
            return task.Result;
        }

        private bool CheckContainerExists(CloudBlobContainer container)
        {
            var task = container.ExistsAsync();
            task.Wait();
            return task.Result;
        }

        private ICloudBlob GetBlobReferenceFromServer(CloudBlobContainer container, string blobName)
        {
            var task = container.GetBlobReferenceFromServerAsync(blobName);
            task.Wait();
            return task.Result;
        }

        private string DownloadText(CloudBlockBlob blob)
        {
            var task = blob.DownloadTextAsync();
            task.Wait();
            return task.Result;
        }

        [Fact(Skip = SkipMessage)]
        public void TestOpenE2E()
        {
            const string Text = "hello";
            var blob = _container.GetBlockBlobReference(HelloFile);
            UploadFromString(blob, Text);
            Assert.True(CheckBlobExists(blob));
            using (var reader = new StreamReader(_fileSystem.Open(PathToFile(HelloFile))))
            {
                string streamText = reader.ReadToEnd();
                Assert.Equal(Text, streamText);
            }
        }

        [Fact(Skip = SkipMessage)]
        public void TestCreateE2E()
        {
            const string Text = "Hello Azure Blob";
            var blob = _container.GetBlockBlobReference(HelloFile);
            Assert.False(CheckBlobExists(blob));
            using (var streamWriter = new StreamWriter(_fileSystem.Create(PathToFile(HelloFile))))
            {
                streamWriter.Write(Text);
            }
            blob = _container.GetBlockBlobReference(HelloFile);
            Assert.True(CheckBlobExists(blob));
            using (var reader = new StreamReader(blob.OpenRead()))
            {
                string streamText = reader.ReadToEnd();
                Assert.Equal(Text, streamText);
            }
        }

        [Fact(Skip = SkipMessage)]
        public void TestDeleteE2E()
        {
            var blob = _container.GetBlockBlobReference(HelloFile);
            UploadFromString(blob, "hello");
            Assert.True(CheckBlobExists(blob));
            _fileSystem.Delete(PathToFile(HelloFile));
            Assert.False(CheckBlobExists(blob));
        }

        [Fact(Skip = SkipMessage)]
        public void TestExistsE2E()
        {
            var helloFilePath = PathToFile(HelloFile);
            var blob = _container.GetBlockBlobReference(HelloFile);
            UploadFromString(blob, "hello");
            Assert.True(_fileSystem.Exists(helloFilePath));
            blob.DeleteIfExistsAsync().Wait();
            Assert.False(_fileSystem.Exists(helloFilePath));
        }

        [Fact(Skip = SkipMessage)]
        public void TestCopyE2E()
        {
            const string SrcFileName = "src";
            const string DestFileName = "dest";
            var srcFilePath = PathToFile(SrcFileName);
            var destFilePath = PathToFile(DestFileName);
            ICloudBlob srcBlob = _container.GetBlockBlobReference(SrcFileName);
            UploadFromString(srcBlob, "hello");
            Assert.True(CheckBlobExists(srcBlob));
            ICloudBlob destBlob = _container.GetBlockBlobReference(DestFileName);
            Assert.False(CheckBlobExists(destBlob));
            _fileSystem.Copy(srcFilePath, destFilePath);
            destBlob = GetBlobReferenceFromServer(_container, DestFileName);
            Assert.True(CheckBlobExists(destBlob));
            srcBlob = GetBlobReferenceFromServer(_container, SrcFileName);
            Assert.True(CheckBlobExists(srcBlob));
            Assert.Equal(DownloadText(_container.GetBlockBlobReference(SrcFileName)), DownloadText(_container.GetBlockBlobReference(DestFileName)));
        }

        [Fact(Skip = SkipMessage)]
        public void TestCopyToLocalE2E()
        {
            var helloFilePath = PathToFile(HelloFile);
            var blob = _container.GetBlockBlobReference(HelloFile);
            var tempFilePath = Path.GetTempFileName();
            const string Text = "hello";
            
            try
            {
                UploadFromString(blob, Text);
                _fileSystem.CopyToLocal(helloFilePath, tempFilePath);
                Assert.True(File.Exists(tempFilePath));
                Assert.Equal(Text, File.ReadAllText(tempFilePath));
            }
            finally
            {
                File.Delete(tempFilePath);
            }
        }

        [Fact(Skip = SkipMessage)]
        public void TestCopyFromLocalE2E()
        {
            var helloFilePath = PathToFile(HelloFile);
            ICloudBlob blob = _container.GetBlockBlobReference(HelloFile);
            Assert.False(CheckBlobExists(blob));
            var tempFilePath = Path.GetTempFileName();
            const string Text = "hello";
            try
            {
                File.WriteAllText(tempFilePath, Text);
                _fileSystem.CopyFromLocal(tempFilePath, helloFilePath);
                blob = GetBlobReferenceFromServer(_container, HelloFile);
                Assert.True(CheckBlobExists(blob));
                using (var stream = new MemoryStream())
                {
                    blob.DownloadToStreamAsync(stream).Wait();
                    stream.Seek(0, SeekOrigin.Begin);

                    using (var sr = new StreamReader(stream))
                    {
                        var matchingText = sr.ReadToEnd();
                        Assert.Equal(Text, matchingText);
                    }
                }
            }
            finally
            {
                File.Delete(tempFilePath);
            }
        }

        [Fact(Skip = SkipMessage)]
        public void TestDeleteDirectoryAtContainerE2E()
        {
            _fileSystem.DeleteDirectory(_container.Uri);
            Assert.False(CheckContainerExists(_container));
        }

        [Fact(Skip = SkipMessage)]
        public void TestDeleteDirectoryFirstLevelE2E()
        {
            const string Directory = "dir";
            var blockBlobs = new List<CloudBlockBlob>(); 

            for (var i = 0; i < 3; i++)
            {
                var filePath = Directory + '/' + i;
                var blockBlob = _container.GetBlockBlobReference(filePath);
                UploadFromString(blockBlob, "hello");
                Assert.True(CheckBlobExists(blockBlob));
                blockBlobs.Add(blockBlob);
            }

            _fileSystem.DeleteDirectory(PathToFile(Directory));

            foreach (var blockBlob in blockBlobs)
            {
                Assert.False(CheckBlobExists(blockBlob));
            }

            Assert.True(CheckContainerExists(_container));
        }

        [Fact(Skip = SkipMessage)]
        public void TestDeleteDirectorySecondLevelE2E()
        {
            const string Directory1 = "dir1";
            const string Directory2 = "dir2";
            var blockBlobs1 = new List<CloudBlockBlob>();
            var blockBlobs2 = new List<CloudBlockBlob>();

            for (var i = 0; i < 3; i++)
            {
                var filePath1 = Directory1 + '/' + i;
                var filePath2 = Directory1 + '/' + Directory2 + '/' + i;
                var blockBlob1 = _container.GetBlockBlobReference(filePath1);
                var blockBlob2 = _container.GetBlockBlobReference(filePath2);
                UploadFromString(blockBlob1, "hello");
                UploadFromString(blockBlob2, "hello");
                Assert.True(CheckBlobExists(blockBlob1));
                Assert.True(CheckBlobExists(blockBlob2));
                blockBlobs1.Add(blockBlob1);
                blockBlobs2.Add(blockBlob2);
            }

            _fileSystem.DeleteDirectory(PathToFile(Directory1 + '/' + Directory2));

            foreach (var blockBlob in blockBlobs2)
            {
                Assert.False(CheckBlobExists(blockBlob));
            }

            foreach (var blockBlob in blockBlobs1)
            {
                Assert.True(CheckBlobExists(blockBlob));
            }

            Assert.True(CheckContainerExists(_container));
        }

        private static void UploadFromString(ICloudBlob blob, string str)
        {
            var byteArray = Encoding.UTF8.GetBytes(str);
            blob.UploadFromByteArrayAsync(byteArray, 0, byteArray.Length).Wait();
        }

        private Uri PathToFile(string filePath)
        {
            return _fileSystem.CreateUriForPath(_container.Name + '/' + filePath);
        }
    }
}
