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

using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Avro;
using Org.Apache.REEF.Client.Avro.AzureBatch;
using Org.Apache.REEF.Client.AzureBatch.Parameters;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Common.Avro;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;
using System;
using System.Collections.Generic;
using System.IO;

namespace Org.Apache.REEF.Client.AzureBatch.Util
{
    internal sealed class JobJarMaker
    {
        private readonly IResourceArchiveFileGenerator _resourceArchiveFileGenerator;
        private readonly DriverFolderPreparationHelper _driverFolderPreparationHelper;
        private readonly AvroAzureBatchJobSubmissionParameters _avroAzureBatchJobSubmissionParameters;
        private readonly REEFFileNames _fileNames;

        [Inject]
        JobJarMaker(
            IResourceArchiveFileGenerator resourceArchiveFileGenerator,
            DriverFolderPreparationHelper driverFolderPreparationHelper,
            REEFFileNames fileNames,
            [Parameter(typeof(AzureBatchAccountName))] string azureBatchAccountName,
            [Parameter(typeof(AzureBatchAccountUri))] string azureBatchAccountUri,
            [Parameter(typeof(AzureBatchPoolId))] string azureBatchPoolId,
            [Parameter(typeof(AzureStorageAccountName))] string azureStorageAccountName,
            [Parameter(typeof(AzureStorageContainerName))] string azureStorageContainerName,
            [Parameter(typeof(AzureBatchPoolDriverPortsList))] List<string> azureBatchPoolDriverPortsList,
            [Parameter(typeof(ContainerRegistryServer))] string containerRegistryServer,
            [Parameter(typeof(ContainerRegistryUsername))] string containerRegistryUsername,
            [Parameter(typeof(ContainerRegistryPassword))] string containerRegistryPassword,
            [Parameter(typeof(ContainerImageName))] string containerImageName)
        {
            _resourceArchiveFileGenerator = resourceArchiveFileGenerator;
            _driverFolderPreparationHelper = driverFolderPreparationHelper;
            _fileNames = fileNames;
            _avroAzureBatchJobSubmissionParameters = new AvroAzureBatchJobSubmissionParameters
            {
                AzureBatchAccountName = azureBatchAccountName,
                AzureBatchAccountUri = azureBatchAccountUri,
                AzureBatchPoolId = azureBatchPoolId,
                AzureStorageAccountName = azureStorageAccountName,
                AzureStorageContainerName = azureStorageContainerName,
                AzureBatchPoolDriverPortsList = azureBatchPoolDriverPortsList,
                ContainerRegistryServer = containerRegistryServer,
                ContainerRegistryUsername = containerRegistryUsername,
                ContainerRegistryPassword = containerRegistryPassword,
                ContainerImageName = containerImageName,
            };
        }

        /// <summary>
        /// Creates a JAR file for the job submission.
        /// </summary>
        /// <param name="jobRequest">Job request received from the client code.</param>
        /// <param name="azureBatchjobId">Azure Batch job Id going to be launched.</param>
        /// <returns>A string path to file.</returns>
        public string CreateJobSubmissionJAR(JobRequest jobRequest, string azureBatchjobId)
        {
            _avroAzureBatchJobSubmissionParameters.sharedJobSubmissionParameters = new AvroJobSubmissionParameters
            {
                jobId = jobRequest.JobIdentifier,
                //// This is dummy in Azure Batch, as it does not use jobSubmissionFolder in Azure Batch.
                jobSubmissionFolder = Path.PathSeparator.ToString()
            };

            string localDriverFolderPath = CreateDriverFolder(azureBatchjobId);

            _driverFolderPreparationHelper.PrepareDriverFolder(jobRequest.AppParameters, localDriverFolderPath);
            SerializeJobFile(localDriverFolderPath, _avroAzureBatchJobSubmissionParameters);

            return _resourceArchiveFileGenerator.CreateArchiveToUpload(localDriverFolderPath);
        }

        private string CreateDriverFolder(string azureBatchjobId)
        {
            return Path.GetFullPath(Path.Combine(Path.GetTempPath(), azureBatchjobId) + Path.DirectorySeparatorChar);
        }

        private void SerializeJobFile(string localDriverFolderPath, AvroAzureBatchJobSubmissionParameters jobParameters)
        {
            var serializedArgs = AvroJsonSerializer<AvroAzureBatchJobSubmissionParameters>.ToBytes(jobParameters);

            var submissionJobArgsFilePath = Path.Combine(localDriverFolderPath,
                    _fileNames.GetReefFolderName(), _fileNames.GetJobSubmissionParametersFile());

            using (var jobArgsFileStream = new FileStream(submissionJobArgsFilePath, FileMode.CreateNew))
            {
                jobArgsFileStream.Write(serializedArgs, 0, serializedArgs.Length);
            }
        }
    }
}
