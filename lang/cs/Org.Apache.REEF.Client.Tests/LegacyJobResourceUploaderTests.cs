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
using System.Threading.Tasks;
using NSubstitute;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Client.Tests
{
    public class LegacyJobResourceUploaderTests
    {
        private const string AnyDriverLocalFolderPath = @"Any\Local\Folder\Path";
        private const string AnyLocalJobFilePath = AnyDriverLocalFolderPath + @"\job-submission-params.json";
        private const string AnyDriverResourceUploadPath = "/vol1/tmp";
        private const string AnyUploadedResourcePath = "hdfs://foo/vol1/tmp/anyFile";
        private const long AnyModificationTime = 1446161745550;
        private const long AnyResourceSize = 53092;

        [Fact]
        public async Task UploadJobResourceCreatesResourceArchive()
        {
            var testContext = new TestContext();
            var jobResourceUploader = testContext.GetJobResourceUploader();
            
            await jobResourceUploader.UploadArchiveResourceAsync(AnyDriverLocalFolderPath, AnyDriverResourceUploadPath);

            // Archive file generator recieved exactly one call with correct driver local folder path with trailing \
            testContext.ResourceArchiveFileGenerator.Received(1).CreateArchiveToUpload(AnyDriverLocalFolderPath + @"\");
        }

        [Fact]
        public async Task UploadJobResourceJavaLauncherCalledWithCorrectArguments()
        {
            var testContext = new TestContext();
            var jobResourceUploader = testContext.GetJobResourceUploader();
            const string anyLocalArchivePath = @"Any\Local\Archive\Path.zip";
            var anyLocalJobFilePath = AnyDriverLocalFolderPath.TrimEnd('\\') + @"\job-submission-params.json";
            testContext.ResourceArchiveFileGenerator.CreateArchiveToUpload(AnyDriverLocalFolderPath + @"\")
                .Returns(anyLocalArchivePath);
            await jobResourceUploader.UploadArchiveResourceAsync(AnyDriverLocalFolderPath, AnyDriverResourceUploadPath);
            await jobResourceUploader.UploadFileResourceAsync(AnyLocalJobFilePath, AnyDriverResourceUploadPath);

            const string javaClassNameForResourceUploader = @"org.apache.reef.bridge.client.JobResourceUploader";
            Guid notUsedGuid;

            // Clientlauncher is called with correct class name, local archive path, upload path and temp file.
            var notUsedTask = testContext.JavaClientLauncher.Received(1)
                .LaunchAsync(
                    JavaLoggingSetting.Info,
                    javaClassNameForResourceUploader,
                    anyLocalArchivePath,
                    "ARCHIVE",
                    AnyDriverResourceUploadPath + "/",
                    Arg.Is<string>(
                        outputFilePath =>
                            Path.GetDirectoryName(outputFilePath) + @"\" == Path.GetTempPath() 
                            && Guid.TryParse(Path.GetFileName(outputFilePath), out notUsedGuid)));

            // Clientlauncher is called with correct class name, local job file path, upload path and temp file.
            notUsedTask = testContext.JavaClientLauncher.Received(1)
                .LaunchAsync(
                    JavaLoggingSetting.Info,    
                    javaClassNameForResourceUploader,
                    anyLocalJobFilePath,
                    "FILE",
                    AnyDriverResourceUploadPath + "/",
                    Arg.Is<string>(
                        outputFilePath =>
                            Path.GetDirectoryName(outputFilePath) + @"\" == Path.GetTempPath()
                            && Guid.TryParse(Path.GetFileName(outputFilePath), out notUsedGuid)));
        }

        [Fact]
        public async Task UploadJobResourceNoFileCreatedByJavaCallThrowsException()
        {
            var testContext = new TestContext();
            var jobResourceUploader = testContext.GetJobResourceUploader(fileExistsReturnValue: false);

            // throws filenotfound exception
            await Assert.ThrowsAsync<FileNotFoundException>(() => jobResourceUploader.UploadArchiveResourceAsync(AnyDriverLocalFolderPath, AnyDriverResourceUploadPath));
        }

        [Fact]
        public async Task UploadJobResourceReturnsJobResourceDetails()
        {
            var testContext = new TestContext();
            var jobResourceUploader = testContext.GetJobResourceUploader();

            var jobResources = new List<JobResource>
            {
                await jobResourceUploader.UploadArchiveResourceAsync(AnyDriverLocalFolderPath, AnyDriverResourceUploadPath),
                await jobResourceUploader.UploadFileResourceAsync(AnyLocalJobFilePath, AnyDriverResourceUploadPath)
            };

            Assert.Equal(jobResources.Count, 2);
            foreach (var resource in jobResources)
            {
                Assert.Equal(AnyModificationTime, resource.LastModificationUnixTimestamp);
                Assert.Equal(AnyResourceSize, resource.ResourceSize);
                Assert.Equal(AnyUploadedResourcePath, resource.RemoteUploadPath);
            }
        }

        private class TestContext
        {
            public readonly IJavaClientLauncher JavaClientLauncher = Substitute.For<IJavaClientLauncher>();
            public readonly IResourceArchiveFileGenerator ResourceArchiveFileGenerator =
                Substitute.For<IResourceArchiveFileGenerator>();

            public LegacyJobResourceUploader GetJobResourceUploader(bool fileExistsReturnValue = true,
                string uploadedResourcePath = AnyUploadedResourcePath,
                long modificationTime = AnyModificationTime,
                long resourceSize = AnyResourceSize)
            {
                var injector = TangFactory.GetTang().NewInjector();
                IFile file = Substitute.For<IFile>();
                IYarnCommandLineEnvironment yarn = Substitute.For<IYarnCommandLineEnvironment>();
                file.Exists(Arg.Any<string>()).Returns(fileExistsReturnValue);
                file.ReadAllText(Arg.Any<string>())
                    .Returns(string.Format("{0};{1};{2}", uploadedResourcePath, modificationTime, resourceSize));
                injector.BindVolatileInstance(GenericType<IJavaClientLauncher>.Class, JavaClientLauncher);
                injector.BindVolatileInstance(GenericType<IResourceArchiveFileGenerator>.Class, ResourceArchiveFileGenerator);
                injector.BindVolatileInstance(GenericType<IFile>.Class, file);
                injector.BindVolatileInstance(GenericType<IYarnCommandLineEnvironment>.Class, yarn);
                return injector.GetInstance<LegacyJobResourceUploader>();
            }
        }
    }
}