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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NSubstitute;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Client.Tests
{
    [TestClass]
    public class LegacyJobResourceUploaderTests
    {
        private const string AnyDriverLocalFolderPath = @"Any\Local\Folder\Path";
        private const string AnyDriverResourceUploadPath = "/vol1/tmp";
        private const string AnyUploadedResourcePath = "hdfs://foo/vol1/tmp/driver.zip";
        private const long AnyModificationTime = 1446161745550;
        private const long AnyResourceSize = 53092;

        [TestMethod]
        public void UploadJobResourceCreatesResourceArchive()
        {
            var testContext = new TestContext();
            var jobResourceUploader = testContext.GetJobResourceUploader();
            
            jobResourceUploader.UploadJobResource(AnyDriverLocalFolderPath, AnyDriverResourceUploadPath);

            // Archive file generator recieved exactly one call with correct driver local folder path with trailing \
            testContext.ResourceArchiveFileGenerator.Received(1).CreateArchiveToUpload(AnyDriverLocalFolderPath + @"\");
        }

        [TestMethod]
        public void UploadJobResourceJavaLauncherCalledWithCorrectArguments()
        {
            var testContext = new TestContext();
            var jobResourceUploader = testContext.GetJobResourceUploader();
            const string anyLocalArchivePath = @"Any\Local\Archive\Path.zip";
            testContext.ResourceArchiveFileGenerator.CreateArchiveToUpload(AnyDriverLocalFolderPath + @"\")
                .Returns(anyLocalArchivePath);
            jobResourceUploader.UploadJobResource(AnyDriverLocalFolderPath, AnyDriverResourceUploadPath);

            const string javaClassNameForResourceUploader = @"org.apache.reef.bridge.client.JobResourceUploader";
            Guid notUsed;

            // Clientlauncher is called with correct class name, local archive path, upload path and temp file.
            testContext.JavaClientLauncher.Received()
                .Launch(javaClassNameForResourceUploader,
                    anyLocalArchivePath,
                    AnyDriverResourceUploadPath + "/",
                    Arg.Is<string>(
                        outputFilePath =>
                            Path.GetDirectoryName(outputFilePath) + @"\" == Path.GetTempPath() 
                            && Guid.TryParse(Path.GetFileName(outputFilePath), out notUsed)));
        }

        [TestMethod]
        [ExpectedException(typeof(FileNotFoundException))]
        public void UploadJobResourceNoFileCreatedByJavaCallThrowsException()
        {
            var testContext = new TestContext();
            var jobResourceUploader = testContext.GetJobResourceUploader(fileExistsReturnValue: false);

            // throws filenotfound exception
            jobResourceUploader.UploadJobResource(AnyDriverLocalFolderPath, AnyDriverResourceUploadPath);
        }

        [TestMethod]
        public void UploadJobResourceReturnsJobResourceDetails()
        {
            var testContext = new TestContext();
            var jobResourceUploader = testContext.GetJobResourceUploader();

            var jobResource = jobResourceUploader.UploadJobResource(AnyDriverLocalFolderPath, AnyDriverResourceUploadPath);

            Assert.AreEqual(AnyModificationTime, jobResource.LastModificationUnixTimestamp);
            Assert.AreEqual(AnyResourceSize, jobResource.ResourceSize);
            Assert.AreEqual(AnyUploadedResourcePath, jobResource.RemoteUploadPath);
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