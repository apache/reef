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
using System.Text.RegularExpressions;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.FileSystem.Hadoop;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.IO.Tests
{
    /// <summary>
    /// Tests for HadoopFileSystem.
    /// </summary>
    /// <see cref="HadoopFileSystem" />    
    public sealed class TestHadoopFileSystem
    {
        private HadoopFileSystem _fileSystem;

        private Uri GetTempUri()
        {
            return
                _fileSystem.CreateUriForPath("/tmp/TestHadoopFileSystem-" +
                        DateTime.Now.ToString("yyyyMMddHHmmssfff"));
        }

        /// <summary>
        /// Sets up the file system instance to be used for the tests.
        /// </summary>
        public TestHadoopFileSystem()
        {
            _fileSystem =
                TangFactory.GetTang()
                    .NewInjector(HadoopFileSystemConfiguration.ConfigurationModule.Build())
                    .GetInstance<HadoopFileSystem>();
        }

        /// <summary>
        /// Creates a temp file locally, uploads it to HDFS and downloads it again.
        /// </summary>
        [Fact(Skip = "These tests need to be run in an environment with HDFS installed.")]
        public void TestCopyFromLocalAndBack()
        {
            var localFile = FileSystemTestUtilities.MakeLocalTempFile();
            var localFileDownloaded = localFile + ".2";
            var remoteUri = GetTempUri();

            _fileSystem.CopyFromLocal(localFile, remoteUri);
            _fileSystem.CopyToLocal(remoteUri, localFileDownloaded);

            Assert.True(File.Exists(localFileDownloaded), "A file up and downloaded should exist on the local file system.");
            Assert.True(FileSystemTestUtilities.HaveSameContent(localFile, localFileDownloaded), "A file up and downloaded should not have changed content.");

            _fileSystem.Delete(remoteUri);
            File.Delete(localFile);
            File.Delete(localFileDownloaded);
        }

        /// <summary>
        /// Tests whether .Exists() works.
        /// </summary>
        [Fact(Skip = "These tests need to be run in an environment with HDFS installed.")]
        public void TestExists()
        {
            var remoteUri = GetTempUri();
            Assert.False(_fileSystem.Exists(remoteUri), "The file should not exist yet");
            var localFile = FileSystemTestUtilities.MakeLocalTempFile();
            _fileSystem.CopyFromLocal(localFile, remoteUri);
            Assert.True(_fileSystem.Exists(remoteUri), "The file should now exist");
            _fileSystem.Delete(remoteUri);
            Assert.False(_fileSystem.Exists(remoteUri), "The file should no longer exist");
            File.Delete(localFile);
        }

        /// <summary>
        /// Tests for .GetChildren().
        /// </summary>
        [Fact(Skip = "These tests need to be run in an environment with HDFS installed.")]
        public void TestGetChildren()
        {
            // Make a directory
            var remoteDirectory = GetTempUri();
            _fileSystem.CreateDirectory(remoteDirectory);

            // Check that it is empty
            Assert.True(_fileSystem.GetChildren(remoteDirectory).Count() == 0, "The directory should be empty.");

            // Upload some localfile there
            var localTempFile = FileSystemTestUtilities.MakeLocalTempFile();
            var remoteUri = new Uri(remoteDirectory, Path.GetFileName(localTempFile));
            _fileSystem.CopyFromLocal(localTempFile, remoteUri);

            // Check that it is on the listing
            var uriInResult = _fileSystem.GetChildren(remoteUri).First();
            Assert.Equal(remoteUri, uriInResult);

            // Download the file and make sure it is the same as before
            var downloadedFileName = localTempFile + ".downloaded";
            _fileSystem.CopyToLocal(uriInResult, downloadedFileName);
            FileSystemTestUtilities.HaveSameContent(localTempFile, downloadedFileName);
            File.Delete(localTempFile);
            File.Delete(downloadedFileName);

            // Delete the file
            _fileSystem.Delete(remoteUri);

            // Check that the folder is empty again
            Assert.True(_fileSystem.GetChildren(remoteDirectory).Count() == 0, "The directory should be empty.");

            // Delete the folder
            _fileSystem.DeleteDirectory(remoteDirectory);
        }

        [Fact(Skip = "These tests need to be run in an environment with HDFS installed.")]
        public void TestOpen()
        {
            // Open() is not supported by HadoopFileSystem. Use CopyToLocal and open the local file instead.
            Assert.Throws<NotImplementedException>(() => _fileSystem.Open(GetTempUri()));
        }

        [Fact(Skip = "These tests need to be run in an environment with HDFS installed.")]
        public void TestCreate()
        {
            // Create() is not supported by HadoopFileSystem. Create a local file and use CopyFromLocal instead.
            Assert.Throws<NotImplementedException>(() => _fileSystem.Create(GetTempUri()));
        }

        [Fact(Skip = "These tests need to be run in an environment with HDFS installed.")]
        public void CreateUriForPathNoPrefix()
        {
            Uri uri = _fileSystem.CreateUriForPath("/tmp/TestHadoop");
            Assert.True(new Regex("hdfs://[a-z]+:\\d+/tmp/TestHadoop").Match(uri.AbsoluteUri).Success);
        }

        [Fact(Skip = "These tests need to be run in an environment with HDFS installed.")]
        public void TestCreateUriForPathInvalid()
        {
            Assert.Throws<ArgumentException>(() => _fileSystem.CreateUriForPath("http://www.invalidhadoopfs.com/container/folder1/file1.txt"));
        }

        [Fact(Skip = "These tests need to be run in an environment with HDFS installed.")]
        public void CreateUriForPathWithPrefix()
        {
            string uriString = "hdfs://localhost:9000/tmp/TestHadoop";
            Assert.Equal(new Uri(uriString), _fileSystem.CreateUriForPath(uriString));
        }

        /// <summary>
        /// This test is to make sure with the HadoopFileSystemConfiguration, HadoopFileSystem can be injected.
        /// </summary>
        [Fact(Skip = "These tests need to be run in an environment with HDFS installed.")]
        public void TestHadoopFileSystemConfiguration()
        {
            var fileSystemTest = TangFactory.GetTang().NewInjector(HadoopFileSystemConfiguration.ConfigurationModule
                .Build())
                .GetInstance<FileSystemTest>();
            Assert.True(fileSystemTest.FileSystem is HadoopFileSystem);
        }
    }

    class FileSystemTest
    {
        public IFileSystem FileSystem { get; private set; }

        [Inject]
        private FileSystemTest(IFileSystem fileSystem)
        {
            FileSystem = fileSystem;
        }
    }
}