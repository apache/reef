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
using Microsoft.Azure.DataLake.Store;
using NSubstitute;
using Org.Apache.REEF.IO.FileSystem.AzureDataLake;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.IO.Tests
{
    public sealed class TestAzureDataLakeFileSystem
    {
        private static readonly Uri FakeBaseUri = new Uri("http://fakeadls.com");
        private static readonly Uri FakeDirUri = new Uri(FakeBaseUri, "dir");
        private static readonly Uri FakeFileUri = new Uri($"{FakeDirUri}/fakefile");
        private readonly TestContext _context = new TestContext();
        private readonly AzureDataLakeFileSystem _fs;

        public TestAzureDataLakeFileSystem()
        {
            _fs = _context.GetAdlsFileSystem();
        }

        [Fact]
        public void TestOpen()
        {
            _context.MockAdlsClient.CreateFile(FakeFileUri.AbsolutePath, IfExists.Overwrite);
            var stream = _fs.Open(FakeBaseUri);
            Assert.IsAssignableFrom<AdlsInputStream>(stream);
        }

        [Fact]
        public void TestOpenException()
        {
            // Open a file that doesn't exist.
            Exception ex = Assert.Throws<AdlsException>(() => _fs.Open(FakeFileUri));
            Assert.IsAssignableFrom<IOException>(ex);
        }

        [Fact]
        public void TestCreate()
        {
            _fs.Create(FakeFileUri);
            Assert.True(_context.MockAdlsClient.CheckExists(FakeFileUri.AbsolutePath));
            var directoryEntry = _context.MockAdlsClient.GetDirectoryEntry(FakeFileUri.AbsolutePath);
            Assert.Equal(DirectoryEntryType.FILE, directoryEntry.Type);
        }

        [Fact]
        public void TestCreateFileUnderDirectory()
        {
            // Checks when file is created, directory in path was properly created too
            _fs.Create(FakeFileUri);
            Assert.True(_context.MockAdlsClient.CheckExists(FakeDirUri.AbsolutePath));
            var directoryEntry = _context.MockAdlsClient.GetDirectoryEntry(FakeDirUri.AbsolutePath);
            Assert.Equal(DirectoryEntryType.DIRECTORY, directoryEntry.Type);
        }

        [Fact]
        public void TestDelete()
        {
            _context.MockAdlsClient.CreateFile(FakeFileUri.AbsolutePath, IfExists.Overwrite);
            _fs.Delete(FakeFileUri);
            Assert.False(_context.MockAdlsClient.CheckExists(FakeFileUri.AbsolutePath));
        }
        
        [Fact]
        public void TestDeleteException()
        {
            // Delete a file that doesn't exist.
            Assert.Throws<IOException>(() => _fs.Delete(FakeFileUri));
        }

        [Fact]
        public void TestFileDoesNotExists()
        {
            Assert.False(_context.GetAdlsFileSystem().Exists(FakeFileUri));
        }

        [Fact]
        public void TestExists()
        {
            _context.MockAdlsClient.CreateFile(FakeFileUri.AbsolutePath, IfExists.Overwrite);
            Assert.True(_fs.Exists(FakeFileUri));
        }

        [Fact]
        public void TestCopy()
        {
            // Setup
            Uri src = new Uri($"{FakeDirUri}/copyfile");
            _context.MockAdlsClient.CreateFile(src.AbsolutePath, IfExists.Fail);
            Assert.True(_context.MockAdlsClient.CheckExists(src.AbsolutePath));
            Assert.False(_context.MockAdlsClient.CheckExists(FakeFileUri.AbsolutePath));

            _fs.Copy(src, FakeFileUri);
            Assert.True(_context.MockAdlsClient.CheckExists(FakeFileUri.AbsolutePath));
        }

        [Fact]
        public void TestCopyException()
        {
            // Source file does not exist
            Uri src = new Uri($"{FakeDirUri}/copyfile");
            Assert.Throws<IOException>(() => _fs.Copy(src, FakeFileUri));
        }

        [Fact(Skip = "This test is failing during appveyor build saying 'Currently not supported for folder' which might be because of a bug in MockAdlsClient.")]
        public void TestCopyFromLocal()
        {
            Assert.False(_context.MockAdlsClient.CheckExists(FakeFileUri.AbsolutePath));
            _fs.CopyFromLocal("fakefile", FakeFileUri);
            Assert.True(_context.MockAdlsClient.CheckExists(FakeFileUri.AbsolutePath));
        }

        [Fact]
        public void TestCopyToLocal()
        {
            _context.MockAdlsClient.CreateFile(FakeFileUri.AbsolutePath, IfExists.Overwrite);
            _fs.CopyToLocal(FakeFileUri, Path.GetFileName(FakeFileUri.LocalPath));
            Assert.True(File.Exists(Path.GetFileName(FakeFileUri.LocalPath)));
        }

        [Fact]
        public void TestCopyToLocalException()
        {
            // Source file does not exist
            Assert.Throws<IOException>(() => _fs.CopyToLocal(FakeFileUri, "fileName"));
        }

        [Fact]
        public void TestCreateDirectory()
        {
            _fs.CreateDirectory(FakeDirUri);
            Assert.True(_context.MockAdlsClient.CheckExists(FakeDirUri.AbsolutePath));
            
            // check if it is a directory and not a file
            var directoryEntry = _context.MockAdlsClient.GetDirectoryEntry(FakeDirUri.AbsolutePath);
            Assert.Equal(DirectoryEntryType.DIRECTORY, directoryEntry.Type); 
        }

        [Fact]
        public void TestDeleteDirectory()
        {
            _context.MockAdlsClient.CreateDirectory(FakeDirUri.AbsolutePath);
            Assert.True(_context.MockAdlsClient.CheckExists(FakeDirUri.AbsolutePath), "Test setup failed: did not successfully create directory to delete.");
            _fs.Delete(FakeDirUri);
            Assert.False(_context.MockAdlsClient.CheckExists(FakeDirUri.AbsolutePath), "Test to delete adls directory failed.");
        }

        [Fact]
        public void TestDeleteDirectoryException()
        {
            // Delete a directory that doesn't exist.
            Assert.Throws<IOException>(() => _fs.DeleteDirectory(FakeDirUri));
        }

        [Fact]
        public void TestGetChildren()
        {
            _context.MockAdlsClient.CreateDirectory(FakeDirUri.AbsolutePath);
            var children = _fs.GetChildren(FakeDirUri);
            int count = children.Count();
            Assert.Equal(0, count);

            _context.MockAdlsClient.CreateFile(FakeFileUri.AbsolutePath, IfExists.Overwrite);
            children = _fs.GetChildren(FakeDirUri);
            count = children.Count();
            Assert.Equal(1, count);
        }

        [Fact]
        public void TestGetChildrenException()
        {
            // Search a directory that doesn't exist.
            Assert.Throws<IOException>(() => _fs.GetChildren(FakeFileUri).ToList());
        }

        [Fact]
        public void TestCreateUriForPath()
        {
            string dirStructure = FakeFileUri.AbsolutePath;
            Uri createdUri = _fs.CreateUriForPath(dirStructure);
            Assert.Equal(createdUri, new Uri($"adl://{_context.AdlsAccountFqdn}{dirStructure}"));
        }

        [Fact]
        public void TestGetFileStatusThrowsException()
        {
            Assert.Throws<ArgumentNullException>(() => _fs.GetFileStatus(null));
        }

        private sealed class TestContext
        {
            public readonly string AdlsAccountFqdn = "adlsAccount.azuredatalakestore.net";
            public readonly AdlsClient MockAdlsClient = Microsoft.Azure.DataLake.Store.MockAdlsFileSystem.MockAdlsClient.GetMockClient();

            public AzureDataLakeFileSystem GetAdlsFileSystem()
            {
                var conf = AzureDataLakeFileSystemConfiguration.ConfigurationModule
                     .Set(AzureDataLakeFileSystemConfiguration.DataLakeStorageAccountFqdn, "adlsAccountFqdn")
                    .Set(AzureDataLakeFileSystemConfiguration.Tenant, "tenant")
                    .Set(AzureDataLakeFileSystemConfiguration.ClientId, "clientId")
                    .Set(AzureDataLakeFileSystemConfiguration.SecretKey, "secretKey")
                    .Build();
                var injector = TangFactory.GetTang().NewInjector(conf);
                var testDataLakeStoreClient = Substitute.For<IDataLakeStoreClient>();
                injector.BindVolatileInstance(testDataLakeStoreClient);
                testDataLakeStoreClient.GetAdlsClient().ReturnsForAnyArgs(MockAdlsClient);
                testDataLakeStoreClient.AccountFqdn.Returns(AdlsAccountFqdn);
                var fs = injector.GetInstance<AzureDataLakeFileSystem>();
                return fs;
            }
        }
    }
}
