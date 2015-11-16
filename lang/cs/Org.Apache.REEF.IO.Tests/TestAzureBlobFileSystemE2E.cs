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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Org.Apache.REEF.IO.FileSystem.AzureBlob;

namespace Org.Apache.REEF.IO.Tests
{
    /// <summary>
    /// E2E tests for AzureBlobFileSystem.
    /// These tests require the person running the test to fill in credentials.
    /// </summary>
    [TestClass, Ignore]
    public sealed class TestAzureBlobFileSystemE2E
    {
        private const string HelloFile = "hello";
        private AzureBlobFileSystem _fileSystem;
        private CloudBlobContainer _container;

        [TestInitialize]
        public void TestInitialize()
        {
            // Fill in before running test!
            const string connectionString = "";
            var defaultContainerName = "reef-test-container-" + Guid.NewGuid();
            _fileSystem = new AzureBlobFileSystem(connectionString);
            _container = CloudStorageAccount.Parse(connectionString).CreateCloudBlobClient().GetContainerReference(defaultContainerName);
            _container.CreateIfNotExists();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            if (_container != null)
            {
                _container.DeleteIfExists();
            }
        }

        [TestMethod]
        public void TestDeleteE2E()
        {
            var blob = _container.GetBlockBlobReference(HelloFile);
            UploadFromString(blob, "hello");
            Assert.IsTrue(blob.Exists());
            _fileSystem.Delete(PathToFile(HelloFile));
            Assert.IsFalse(blob.Exists());
        }

        [TestMethod]
        public void TestExistsE2E()
        {
            var helloFilePath = PathToFile(HelloFile);
            var blob = _container.GetBlockBlobReference(HelloFile);
            UploadFromString(blob, "hello");
            Assert.IsTrue(_fileSystem.Exists(helloFilePath));
            blob.DeleteIfExists();
            Assert.IsFalse(_fileSystem.Exists(helloFilePath));
        }

        [TestMethod]
        public void TestCopyE2E()
        {
            const string SrcFileName = "src";
            const string DestFileName = "dest";
            var srcFilePath = PathToFile(SrcFileName);
            var destFilePath = PathToFile(DestFileName);
            ICloudBlob srcBlob = _container.GetBlockBlobReference(SrcFileName);
            UploadFromString(srcBlob, "hello");
            Assert.IsTrue(srcBlob.Exists());
            ICloudBlob destBlob = _container.GetBlockBlobReference(DestFileName);
            Assert.IsFalse(destBlob.Exists());
            _fileSystem.Copy(srcFilePath, destFilePath);
            destBlob = _container.GetBlobReferenceFromServer(DestFileName);
            Assert.IsTrue(destBlob.Exists());
            srcBlob = _container.GetBlobReferenceFromServer(SrcFileName);
            Assert.IsTrue(srcBlob.Exists());
            Assert.AreEqual(_container.GetBlockBlobReference(SrcFileName).DownloadText(), _container.GetBlockBlobReference(DestFileName).DownloadText());
        }

        [TestMethod]
        public void TestCopyToLocalE2E()
        {
            var helloFilePath = PathToFile(HelloFile);
            var blob = _container.GetBlockBlobReference(HelloFile);
            var tempFilePath = GetTempFilePath();
            const string Text = "hello";
            try
            {
                UploadFromString(blob, Text);
                _fileSystem.CopyToLocal(helloFilePath, tempFilePath);
                Assert.IsTrue(File.Exists(tempFilePath));
                Assert.AreEqual(File.ReadAllText(tempFilePath), Text);
            }
            finally
            {
                File.Delete(tempFilePath);
            }
        }

        [TestMethod]
        public void TestCopyFromLocalE2E()
        {
            var helloFilePath = PathToFile(HelloFile);
            ICloudBlob blob = _container.GetBlockBlobReference(HelloFile);
            Assert.IsFalse(blob.Exists());
            var tempFilePath = GetTempFilePath();
            const string Text = "hello";
            try
            {
                File.WriteAllText(tempFilePath, Text);
                _fileSystem.CopyFromLocal(tempFilePath, helloFilePath);
                blob = _container.GetBlobReferenceFromServer(HelloFile);
                Assert.IsTrue(blob.Exists());
                using (var stream = new MemoryStream())
                {
                    blob.DownloadToStream(stream);
                    stream.Seek(0, SeekOrigin.Begin);

                    using (var sr = new StreamReader(stream))
                    {
                        var matchingText = sr.ReadToEnd();
                        Assert.AreEqual(matchingText, Text);
                    }
                }
            }
            finally
            {
                File.Delete(tempFilePath);
            }
        }

        

        [TestMethod]
        public void TestDeleteDirectoryAtContainerE2E()
        {
            _fileSystem.DeleteDirectory(_container.Uri);
            Assert.IsFalse(_container.Exists());
        }

        [TestMethod]
        public void TestDeleteDirectoryFirstLevelE2E()
        {
            const string Directory = "dir";
            var blockBlobs = new List<CloudBlockBlob>(); 

            for (var i = 0; i < 3; i++)
            {
                var filePath = Directory + '/' + i;
                var blockBlob = _container.GetBlockBlobReference(filePath);
                UploadFromString(blockBlob, "hello");
                Assert.IsTrue(blockBlob.Exists());
                blockBlobs.Add(blockBlob);
            }

            _fileSystem.DeleteDirectory(PathToFile(Directory));

            foreach (var blockBlob in blockBlobs)
            {
                Assert.IsFalse(blockBlob.Exists());
            }

            Assert.IsTrue(_container.Exists());
        }

        [TestMethod]
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
                Assert.IsTrue(blockBlob1.Exists());
                Assert.IsTrue(blockBlob2.Exists());
                blockBlobs1.Add(blockBlob1);
                blockBlobs2.Add(blockBlob2);
            }

            _fileSystem.DeleteDirectory(PathToFile(Directory1 + '/' + Directory2));

            foreach (var blockBlob in blockBlobs2)
            {
                Assert.IsFalse(blockBlob.Exists());
            }

            foreach (var blockBlob in blockBlobs1)
            {
                Assert.IsTrue(blockBlob.Exists());
            }

            Assert.IsTrue(_container.Exists());
        }

        private static string GetTempFilePath()
        {
            return Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        }

        private static void UploadFromString(ICloudBlob blob, string str)
        {
            var byteArray = Encoding.UTF8.GetBytes(str);
            blob.UploadFromByteArray(byteArray, 0, byteArray.Length);
        }

        private Uri PathToFile(string filePath)
        {
            return _fileSystem.CreateUriForPath(_container.Name + '/' + filePath);
        }
    }
}
