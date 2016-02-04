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
using System.Linq;
using NSubstitute;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.YARN.RestClient;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Client.Tests
{
    public class JobResourceUploaderTests
    {
        private const string AnyDriverLocalFolderPath = @"Any\Local\Folder\Path\";
        private const string AnyDriverResourceUploadPath = "/vol1/tmp/";
        private const string AnyUploadedResourcePath = "/vol1/tmp/Path.zip";
        private const string AnyJobFileResourcePath = "/vol1/tmp/job-submission-params.json";
        private const string AnyHost = "host";
        private const string AnyScheme = "hdfs://";
        private const string AnyUploadedResourceAbsoluteUri = AnyScheme + AnyHost + AnyUploadedResourcePath;
        private const string AnyJobFileResourceAbsoluteUri = AnyScheme + AnyHost + AnyJobFileResourcePath;
        private const string AnyLocalArchivePath = @"Any\Local\Archive\Path.zip";
        private const string AnyLocalJobFilePath = @"Any\Local\Folder\Path\job-submission-params.json";
        private const long AnyModificationTime = 1447413621;
        private const long AnyResourceSize = 53092;
        private static readonly DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, 0);

        [Fact]
        public void JobResourceUploaderCanInstantiateWithDefaultBindings()
        {
            TangFactory.GetTang().NewInjector().GetInstance<FileSystemJobResourceUploader>();
        }

        [Fact]
        public void UploadJobResourceCreatesResourceArchive()
        {
            var testContext = new TestContext();
            var jobResourceUploader = testContext.GetJobResourceUploader();

            jobResourceUploader.UploadArchiveResource(AnyDriverLocalFolderPath, AnyDriverResourceUploadPath);

            // Archive file generator recieved exactly one call with correct driver local folder path
            testContext.ResourceArchiveFileGenerator.Received(1).CreateArchiveToUpload(AnyDriverLocalFolderPath);
        }

        [Fact]
        public void UploadJobResourceReturnsJobResourceDetails()
        {
            var testContext = new TestContext();
            var jobResourceUploader = testContext.GetJobResourceUploader();

            var archiveJobResource = jobResourceUploader.UploadArchiveResource(AnyDriverLocalFolderPath, AnyDriverResourceUploadPath);
            var fileJobResource = jobResourceUploader.UploadFileResource(AnyLocalJobFilePath, AnyDriverResourceUploadPath);
            var jobResources = new List<JobResource> { archiveJobResource, fileJobResource };

            foreach (var resource in jobResources)
            {
                Assert.Equal(AnyModificationTime, resource.LastModificationUnixTimestamp);
                Assert.Equal(AnyResourceSize, resource.ResourceSize);
            }

            var resourcePaths = new HashSet<string>(jobResources.Select(resource => resource.RemoteUploadPath));
            Assert.True(resourcePaths.Contains(AnyUploadedResourceAbsoluteUri));
            Assert.True(resourcePaths.Contains(AnyJobFileResourceAbsoluteUri));
        }

        [Fact]
        public void UploadJobResourceMakesCorrectFileSystemCalls()
        {
            var testContext = new TestContext();
            var jobResourceUploader = testContext.GetJobResourceUploader();

            jobResourceUploader.UploadArchiveResource(AnyDriverLocalFolderPath, AnyDriverResourceUploadPath);
            jobResourceUploader.UploadFileResource(AnyLocalJobFilePath, AnyDriverResourceUploadPath);

            testContext.FileSystem.Received(1).CreateUriForPath(AnyUploadedResourcePath);

            testContext.FileSystem.Received(2).CreateUriForPath(AnyDriverResourceUploadPath);
            testContext.FileSystem.Received(1)
                .CopyFromLocal(AnyLocalArchivePath, new Uri(AnyUploadedResourceAbsoluteUri));
            testContext.FileSystem.Received(2)
                .CreateDirectory(new Uri(AnyScheme + AnyHost + AnyDriverResourceUploadPath));

            testContext.FileSystem.Received(1).CreateUriForPath(AnyJobFileResourcePath);
            testContext.FileSystem.Received(1)
                .CopyFromLocal(AnyLocalJobFilePath, new Uri(AnyJobFileResourceAbsoluteUri));
        }

        private class TestContext
        {
            public readonly IResourceArchiveFileGenerator ResourceArchiveFileGenerator =
                Substitute.For<IResourceArchiveFileGenerator>();
            public readonly IFileSystem FileSystem = Substitute.For<IFileSystem>();

            public FileSystemJobResourceUploader GetJobResourceUploader()
            {
                var injector = TangFactory.GetTang().NewInjector();
                FileSystem.GetFileStatus(new Uri(AnyUploadedResourceAbsoluteUri))
                    .Returns(new FileStatus(Epoch + TimeSpan.FromSeconds(AnyModificationTime), AnyResourceSize));
                FileSystem.GetFileStatus(new Uri(AnyJobFileResourceAbsoluteUri))
                    .Returns(new FileStatus(Epoch + TimeSpan.FromSeconds(AnyModificationTime), AnyResourceSize));
                ResourceArchiveFileGenerator.CreateArchiveToUpload(AnyDriverLocalFolderPath)
                    .Returns(AnyLocalArchivePath);
                FileSystem.CreateUriForPath(AnyDriverResourceUploadPath)
                    .Returns(new Uri(AnyScheme + AnyHost + AnyDriverResourceUploadPath));
                FileSystem.CreateUriForPath(AnyUploadedResourcePath)
                    .Returns(new Uri(AnyUploadedResourceAbsoluteUri));
                FileSystem.CreateUriForPath(AnyJobFileResourcePath)
                    .Returns(new Uri(AnyJobFileResourceAbsoluteUri));
                IFile file = Substitute.For<IFile>();
                file.Exists(Arg.Any<string>()).Returns(true);
                injector.BindVolatileInstance(GenericType<IFile>.Class, file);
                injector.BindVolatileInstance(GenericType<IResourceArchiveFileGenerator>.Class, ResourceArchiveFileGenerator);
                injector.BindVolatileInstance(GenericType<IFileSystem>.Class, FileSystem);
                return injector.GetInstance<FileSystemJobResourceUploader>();
            }
        }
    }
}