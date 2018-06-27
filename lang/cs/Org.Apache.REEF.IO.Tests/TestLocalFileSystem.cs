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
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.FileSystem.Local;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.IO.Tests
{
    /// <summary>
    /// Tests for Org.Apache.REEF.IO.FileSystem.Local.LocalFileSystem
    /// </summary>
    public sealed class TestLocalFileSystem
    {
        private const string TempFileName = "REEF.TestLocalFileSystem.tmp";
        private const byte TestByte = 123;

        [Fact]
        public void TestCreateAndOpenAndDelete()
        {
            var fs = GetFileSystem();

            // Create a temp file
            var tempFilePath = Path.Combine(Path.GetTempPath(), TempFileName);
            var tempFileUri = new Uri(tempFilePath);

            using (var s = fs.Create(tempFileUri))
            {
                s.WriteByte(TestByte);
            }

            Assert.True(fs.Exists(tempFileUri));
            Assert.True(File.Exists(tempFilePath));

            // Make sure it was read correctly
            using (var s = fs.Open(tempFileUri))
            {
                Assert.Equal(TestByte, s.ReadByte());
            }
            using (var s = File.Open(tempFilePath, FileMode.Open))
            {
                Assert.Equal(TestByte, s.ReadByte());
            }

            // Delete it
            fs.Delete(tempFileUri);

            Assert.False(fs.Exists(tempFileUri));
            Assert.False(File.Exists(tempFilePath));
        }

        [Fact]
        public void TestCopyFromLocal()
        {
            var fs = GetFileSystem();
            var sourceFilePath = Path.Combine(Path.GetTempPath(), TempFileName);
            MakeLocalTestFile(sourceFilePath);

            var destinationFilePath = sourceFilePath + ".copy";
            if (File.Exists(destinationFilePath))
            {
                File.Delete(destinationFilePath);
            }

            var destinationUri = new Uri(destinationFilePath);
            fs.CopyFromLocal(sourceFilePath, destinationUri);
            TestRemoteFile(fs, destinationUri);

            fs.Delete(destinationUri);
            Assert.False(fs.Exists(destinationUri));

            File.Delete(sourceFilePath);
            Assert.False(File.Exists(sourceFilePath));
        }

        [Fact]
        public void TestCopyToLocal()
        {
            var fs = GetFileSystem();
            var sourceFilePath = Path.Combine(Path.GetTempPath(), TempFileName);
            var sourceUri = new Uri(sourceFilePath);
            var destinationFilePath = sourceFilePath + ".copy";
            if (File.Exists(destinationFilePath))
            {
                File.Delete(destinationFilePath);
            }

            MakeRemoteTestFile(fs, sourceUri);
            fs.CopyToLocal(sourceUri, destinationFilePath);
            TestLocalFile(destinationFilePath);

            fs.Delete(sourceUri);
            Assert.False(fs.Exists(sourceUri));

            File.Delete(destinationFilePath);
            Assert.False(File.Exists(destinationFilePath));
        }

        [Fact]
        public void TestCopy()
        {
            var fs = GetFileSystem();
            var sourcePath = Path.Combine(Path.GetTempPath(), TempFileName);
            var sourceUri = new Uri(sourcePath);
            var destinationUri = new Uri(sourcePath + ".copy");
            MakeRemoteTestFile(fs, sourceUri);
            if (fs.Exists(destinationUri))
            {
                fs.Delete(destinationUri);
            }
            Assert.False(fs.Exists(destinationUri));
            fs.Copy(sourceUri, destinationUri);
            Assert.True(fs.Exists(destinationUri));
            TestRemoteFile(fs, destinationUri);
            fs.Delete(destinationUri);
            Assert.False(fs.Exists(destinationUri));
            fs.Delete(sourceUri);
            Assert.False(fs.Exists(sourceUri));
        }

        [Fact]
        public void TestIsDirectory()
        {
            var fs = GetFileSystem();
            // Directory check
            var directoryUri = new Uri(Path.Combine(Path.GetTempPath(), TempFileName) + "/");
            fs.CreateDirectory(directoryUri);
            Assert.True(fs.IsDirectory(directoryUri));

            // File check
            var fileUri = new Uri(directoryUri, "testfile");
            MakeRemoteTestFile(fs, fileUri);
            Assert.False(fs.IsDirectory(fileUri));

            // Fake directory check
            var fakeDirectoryUri = new Uri(Path.Combine(Path.GetTempPath(), "fakeDir") + "/");
            Assert.False(fs.IsDirectory(fakeDirectoryUri));

            // Clean up
            fs.Delete(fileUri);
            fs.DeleteDirectory(directoryUri);
        }

        [Fact]
        public void TestGetChildren()
        {
            var fs = GetFileSystem();
            var directoryUri = new Uri(Path.Combine(Path.GetTempPath(), TempFileName) + "/");
            fs.CreateDirectory(directoryUri);
            var fileUri = new Uri(directoryUri, "testfile");
            
            MakeRemoteTestFile(fs, fileUri);
            var fileUris = fs.GetChildren(directoryUri).ToList();
            foreach (var uri in fileUris)
            {
                TestRemoteFile(fs, uri);
            }

            Assert.Equal(1, fileUris.Count);
            Assert.Equal(fileUri, fileUris[0]);
            fs.Delete(fileUri);
            fs.DeleteDirectory(directoryUri);
        }

        [Fact]
        public void TestDeleteDirectory()
        {
            var fs = GetFileSystem();
            // Create directory
            var directoryUri = new Uri(Path.Combine(Path.GetTempPath(), TempFileName) + "/");
            fs.CreateDirectory(directoryUri);

            // Create sub directory
            string childDirName = "childDir";
            var childDirUri = new Uri(directoryUri, childDirName);
            fs.CreateDirectory(childDirUri);

            // Create file
            const string fileName = "testfile";
            var fileUri = new Uri(directoryUri, fileName);
            MakeRemoteTestFile(fs, fileUri);

            //Create sub directory file      
            var childDirFileUri = new Uri(directoryUri, $"{childDirName}/{fileName}");
            MakeRemoteTestFile(fs, childDirFileUri);

            fs.DeleteDirectory(directoryUri);

            Assert.False(File.Exists(directoryUri.AbsolutePath));
        }

        private IFileSystem GetFileSystem()
        {
            return TangFactory.GetTang()
                .NewInjector(LocalFileSystemConfiguration.ConfigurationModule.Build())
                .GetInstance<IFileSystem>();
        }

        private void MakeRemoteTestFile(IFileSystem fs, Uri path)
        {
            if (fs.Exists(path))
            {
                fs.Delete(path);
            }
            using (var s = fs.Create(path))
            {
                s.WriteByte(TestByte);
            }
        }

        private void MakeLocalTestFile(string filePath)
        {
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }
            using (var s = File.Create(filePath))
            {
                s.WriteByte(TestByte);
            }
        }

        private void TestRemoteFile(IFileSystem fs, Uri path)
        {
            using (var s = fs.Open(path))
            {
                Assert.Equal(TestByte, s.ReadByte());
            }
        }

        private void TestLocalFile(string filePath)
        {
            using (var s = File.Open(filePath, FileMode.Open))
            {
                Assert.Equal(TestByte, s.ReadByte());
            }
        }
    }
}