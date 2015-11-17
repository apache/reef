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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using NSubstitute;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.FileSystem.AzureBlob;
using Org.Apache.REEF.IO.FileSystem.AzureBlob.RetryPolicy;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IO.Tests
{
    /// <summary>
    /// Test class for unit testing the AzureBlockBlobFileSystem.
    /// Some methods are currently not unit tested due to the complexity of "faking out"
    /// the methods. They are instead tested E2E in TestAzureBlockBlobFileSystemE2E
    /// </summary>
    [TestClass]
    public sealed class TestAzureBlockBlobFileSystem
    {
        private readonly static Uri FakeUri = new Uri("http://fake.com");

        [TestMethod, ExpectedException(typeof(NotSupportedException))]
        public void TestCreateNotSupported()
        {
            new TestContext().GetAzureFileSystem().Create(FakeUri);
        }

        [TestMethod, ExpectedException(typeof(NotSupportedException))]
        public void TestOpenNotSupported()
        {
            new TestContext().GetAzureFileSystem().Open(FakeUri);
        }

        [TestMethod]
        public void TestDelete()
        {
            var testContext = new TestContext();
            testContext.GetAzureFileSystem().Delete(new Uri(FakeUri, "container/file"));
            testContext.TestCloudBlockBlob.Received(1).Delete();
        }

        [TestMethod]
        public void TestExists()
        {
            var testContext = new TestContext();
            testContext.GetAzureFileSystem().Exists(FakeUri);
            testContext.TestCloudBlockBlob.Received(1).Exists();
        }

        [TestMethod]
        public void TestCopyToLocal()
        {
            var testContext = new TestContext();
            testContext.GetAzureFileSystem().CopyToLocal(FakeUri, "local");
            testContext.TestCloudBlockBlob.Received(1).DownloadToFile("local", FileMode.CreateNew);
        }

        [TestMethod]
        public void TestCopyFromLocal()
        {
            var testContext = new TestContext();
            testContext.GetAzureFileSystem().CopyFromLocal("local", FakeUri);
            testContext.TestCloudBlockBlob.Received(1).UploadFromFile("local", FileMode.Open);
        }

        [TestMethod]
        public void TestCreateDirectory()
        {
            var testContext = new TestContext();
            testContext.GetAzureFileSystem().CreateDirectory(FakeUri);
            testContext.TestCloudBlobClient.DidNotReceiveWithAnyArgs();
        }

        [TestMethod, ExpectedException(typeof(StorageException))]
        public void TestDeleteDirectoryFails()
        {
            new TestContext().GetAzureFileSystem().DeleteDirectory(FakeUri);
        }

        [TestMethod]
        public void TestDeleteDirectoryAtContainer()
        {
            var testContext = new TestContext();
            testContext.GetAzureFileSystem().DeleteDirectory(new Uri("http://test.com/test"));
            testContext.TestCloudBlobClient.Received(1).GetContainerReference("test");
            testContext.TestCloudBlobContainer.Received(1).DeleteIfExists();
        }

        [TestMethod]
        public void TestDeleteDirectoryRecursive()
        {
            var testContext = new TestContext();
            testContext.TestCloudBlobDirectory.ListBlobs(true).ReturnsForAnyArgs(Enumerable.Repeat(testContext.TestCloudBlob, 5));
            testContext.GetAzureFileSystem().DeleteDirectory(new Uri("http://test.com/container/directory/directory"));
            testContext.TestCloudBlobClient.Received(1).GetContainerReference("container");
            testContext.TestCloudBlobContainer.Received(1).GetDirectoryReference("directory");
            testContext.TestCloudBlobDirectory.Received(1).GetDirectoryReference("directory");
            testContext.TestCloudBlobDirectory.Received(1).ListBlobs(true);
            testContext.TestCloudBlob.Received(5).DeleteIfExists();
        }

        [TestMethod]
        public void TestCreateUriForPath()
        {
            var testContext = new TestContext();
            const string dirStructure = "container/directory";
            Assert.AreEqual(new Uri(FakeUri, dirStructure), testContext.GetAzureFileSystem().CreateUriForPath(dirStructure));
        }

        private sealed class TestContext
        {
            public readonly ICloudBlobClient TestCloudBlobClient = Substitute.For<ICloudBlobClient>();
            public readonly ICloudBlob TestCloudBlob = Substitute.For<ICloudBlob>();
            public readonly ICloudBlockBlob TestCloudBlockBlob = Substitute.For<ICloudBlockBlob>();
            public readonly ICloudBlobContainer TestCloudBlobContainer = Substitute.For<ICloudBlobContainer>();
            public readonly ICloudBlobDirectory TestCloudBlobDirectory = Substitute.For<ICloudBlobDirectory>();

            public IFileSystem GetAzureFileSystem()
            {
                var conf = AzureBlockBlobFileSystemConfiguration.ConfigurationModule
                    .Set(AzureBlockBlobFileSystemConfiguration.ConnectionString, "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;")
                    .Build();

                var injector = TangFactory.GetTang().NewInjector(conf);
                injector.BindVolatileInstance(TestCloudBlobClient);
                var fs = injector.GetInstance<AzureBlockBlobFileSystem>();
                TestCloudBlobClient.BaseUri.ReturnsForAnyArgs(FakeUri);
                TestCloudBlobClient.GetBlockBlobReference(FakeUri).ReturnsForAnyArgs(TestCloudBlockBlob);
                TestCloudBlobClient.GetContainerReference("container").ReturnsForAnyArgs(TestCloudBlobContainer);
                TestCloudBlobContainer.GetDirectoryReference("directory").ReturnsForAnyArgs(TestCloudBlobDirectory);
                TestCloudBlobDirectory.GetDirectoryReference("directory").ReturnsForAnyArgs(TestCloudBlobDirectory);
                return fs;
            }
        }
    }
}
