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
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NSubstitute;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.FileSystem.AzureBlob;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.IO.Tests
{
    /// <summary>
    /// Test class for unit testing the AzureBlockBlobFileSystem.
    /// Some methods are currently not unit tested due to the complexity of "faking out"
    /// the methods. They are instead tested E2E in TestAzureBlockBlobFileSystemE2E
    /// </summary>
    public sealed class TestAzureBlockBlobFileSystem
    {
        private static readonly Uri FakeUri = new Uri("http://storageacct.blob.core.windows.net/container/file");
        private TestContext testContext;

        private static Uri BaseUri
        {
            get { return new Uri(FakeUri.GetLeftPart(UriPartial.Authority)); }
        }

        public TestAzureBlockBlobFileSystem()
        {
            this.testContext = new TestContext();
        }

        [Fact]
        public void TestCreate()
        {
            Stream stream = testContext.GetAzureFileSystem().Create(FakeUri);
            testContext.TestCloudBlockBlob.Received(1).Create();
            Assert.Equal(testContext.TestCreateStream, stream);
        }

        [Fact]
        public void TestOpen()
        {
            Stream stream = testContext.GetAzureFileSystem().Open(FakeUri);
            testContext.TestCloudBlockBlob.Received(1).Open();
            Assert.Equal(testContext.TestOpenStream, stream);
        }

        [Fact]
        public void TestDelete()
        {
            testContext.GetAzureFileSystem().Delete(FakeUri);
            testContext.TestCloudBlockBlob.Received(1).Delete();
        }

        [Fact]
        public void TestExists()
        {
            testContext.GetAzureFileSystem().Exists(FakeUri);
            testContext.TestCloudBlockBlob.Received(1).Exists();
        }

        [Fact]
        public void TestCopyToLocal()
        {
            testContext.GetAzureFileSystem().CopyToLocal(FakeUri, "local");
            testContext.TestCloudBlockBlob.Received(1).DownloadToFile("local", FileMode.CreateNew);
        }

        [Fact]
        public void TestCopyFromLocal()
        {
            testContext.GetAzureFileSystem().CopyFromLocal("local", FakeUri);
            testContext.TestCloudBlockBlob.Received(1).UploadFromFile("local", FileMode.Open);
        }

        [Fact]
        public void TestCreateDirectory()
        {
            var testContext = new TestContext();
            testContext.GetAzureFileSystem().CreateDirectory(FakeUri);
            testContext.TestCloudBlobClient.DidNotReceiveWithAnyArgs();
        }

        [Fact]
        public void TestDeleteDirectoryFails()
        {
            Assert.Throws<StorageException>(() => new TestContext().GetAzureFileSystem().DeleteDirectory(new Uri(FakeUri.GetLeftPart(UriPartial.Authority))));
        }

        [Fact]
        public void TestDeleteDirectoryAtContainer()
        {
            var testContext = new TestContext();
            testContext.GetAzureFileSystem().DeleteDirectory(new Uri("http://test.com/test"));
            testContext.TestCloudBlobClient.Received(1).GetContainerReference("test");
            testContext.TestCloudBlobContainer.Received(1).DeleteIfExists();
        }

        [Fact]
        public void TestDeleteDirectoryRecursive()
        {
            testContext.TestCloudBlobDirectory.ListBlobs(true).ReturnsForAnyArgs(Enumerable.Repeat(testContext.TestCloudBlob, 5));
            testContext.GetAzureFileSystem().DeleteDirectory(new Uri("http://test.com/container/directory/directory"));
            testContext.TestCloudBlobClient.Received(1).GetContainerReference("container");
            testContext.TestCloudBlobContainer.Received(1).GetDirectoryReference("directory");
            testContext.TestCloudBlobDirectory.Received(1).GetDirectoryReference("directory");
            testContext.TestCloudBlobDirectory.Received(1).ListBlobs(true);
            testContext.TestCloudBlob.Received(5).DeleteIfExistsAsync();
        }

        [Fact]
        public void TestCreateUriForAbsolutePathInvalid()
        {
            Assert.Throws<ArgumentException>(() => testContext.GetAzureFileSystem().CreateUriForPath("http://www.invalidstorageaccount.com/container/folder1/file1.txt"));
        }

        [Fact]
        public void TestCreateUriForAbsolutePath()
        {
            Uri uri = new Uri("http://storageacct.blob.core.windows.net/container/folder1/folder2/file.txt");
            Uri resultUri = testContext.GetAzureFileSystem().CreateUriForPath(uri.AbsoluteUri);
            Assert.Equal<Uri>(uri, resultUri);
        }

        [Fact]
        public void TestCreateUriForRelativePathValid()
        {
            string relativePath = "container/folder1/folder2/file.txt";
            Uri expectedUri = new Uri(BaseUri, relativePath);
            Uri resultUri = testContext.GetAzureFileSystem().CreateUriForPath(relativePath);
            Assert.Equal<Uri>(expectedUri, resultUri);
        }

        [Fact]
        public void TestCreateUriForRelativePathWithContainerNameTooSmall()
        {
            // Container name must be atleast 3 characters.
            string relativePath = "c1/folder1/folder2/file.txt";
            Uri expectedUri = new Uri(BaseUri, relativePath);
            Assert.Throws<ArgumentException>(() => testContext.GetAzureFileSystem().CreateUriForPath(relativePath));
        }

        [Fact]
        public void TestCreateUriForRelativePathWithInvalidContainerName()
        {
            // Container name cannot contain a colon character.
            string relativePath = "c:/folder1/folder2/file.txt";
            Assert.Throws<ArgumentException>(() => testContext.GetAzureFileSystem().CreateUriForPath(relativePath));
        }

        private sealed class TestContext
        {
            public readonly ICloudBlobClient TestCloudBlobClient = Substitute.For<ICloudBlobClient>();
            public readonly ICloudBlob TestCloudBlob = Substitute.For<ICloudBlob>();
            public readonly ICloudBlockBlob TestCloudBlockBlob = Substitute.For<ICloudBlockBlob>();
            public readonly ICloudBlobContainer TestCloudBlobContainer = Substitute.For<ICloudBlobContainer>();
            public readonly ICloudBlobDirectory TestCloudBlobDirectory = Substitute.For<ICloudBlobDirectory>();
            public readonly Stream TestOpenStream = Substitute.For<Stream>();
            public readonly Stream TestCreateStream = Substitute.For<Stream>();

            public IFileSystem GetAzureFileSystem()
            {
                var conf = AzureBlockBlobFileSystemConfiguration.ConfigurationModule
                    .Set(AzureBlockBlobFileSystemConfiguration.ConnectionString, "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;")
                    .Build();

                var injector = TangFactory.GetTang().NewInjector(conf);
                injector.BindVolatileInstance(TestCloudBlobClient);
                var fs = injector.GetInstance<AzureBlockBlobFileSystem>();
                TestCloudBlobClient.BaseUri.ReturnsForAnyArgs(BaseUri);
                TestCloudBlockBlob.Open().Returns(TestOpenStream);
                TestCloudBlockBlob.Create().Returns(TestCreateStream);
                TestCloudBlockBlob.Blob.ReturnsForAnyArgs(new CloudBlockBlob(FakeUri));
                TestCloudBlobClient.GetBlockBlobReference(FakeUri).ReturnsForAnyArgs(TestCloudBlockBlob);
                TestCloudBlobClient.GetContainerReference("container").ReturnsForAnyArgs(TestCloudBlobContainer);
                TestCloudBlobContainer.GetDirectoryReference("directory").ReturnsForAnyArgs(TestCloudBlobDirectory);
                TestCloudBlobDirectory.GetDirectoryReference("directory").ReturnsForAnyArgs(TestCloudBlobDirectory);
                return fs;
            }
        }
    }
}
