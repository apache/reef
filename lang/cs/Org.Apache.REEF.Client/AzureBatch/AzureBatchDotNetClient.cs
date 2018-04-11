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
using System.Threading.Tasks;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.AzureBatch.Storage;
using Org.Apache.REEF.Client.AzureBatch;
using Org.Apache.REEF.Client.AzureBatch.Util;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.DotNet.AzureBatch
{
    public sealed class AzureBatchDotNetClient : IREEFClient
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(AzureBatchDotNetClient));

        /// Maximum number of characters allowed in Azure Batch job name. This limit is imposed by Azure Batch.
        private static readonly int AzureBatchMaxCharsJobName = 64;

        private readonly IInjector _injector;
        private readonly DriverFolderPreparationHelper _driverFolderPreparationHelper;
        private readonly REEFFileNames _fileNames;
        private readonly AzureStorageClient _azureStorageClient;
        private readonly JobRequestBuilderFactory _jobRequestBuilderFactory;
        private readonly AzureBatchService _batchService;
        private readonly JobJarMaker _jobJarMaker;
        private readonly AzureBatchFileNames _azbatchFileNames;

        [Inject]
        private AzureBatchDotNetClient(
            IInjector injector,
            IResourceArchiveFileGenerator resourceArchiveFileGenerator,
            DriverFolderPreparationHelper driverFolderPreparationHelper,
            AzureStorageClient azureStorageClient,
            REEFFileNames fileNames,
            AzureBatchFileNames azbatchFileNames,
            JobRequestBuilderFactory jobRequestBuilderFactory,
            AzureBatchService batchService,
            JobJarMaker jobJarMaker)
        {
            _injector = injector;
            _fileNames = fileNames;
            _azbatchFileNames = azbatchFileNames;
            _driverFolderPreparationHelper = driverFolderPreparationHelper;
            _azureStorageClient = azureStorageClient;
            _jobRequestBuilderFactory = jobRequestBuilderFactory;
            _batchService = batchService;
            _jobJarMaker = jobJarMaker;
        }

        public JobRequestBuilder NewJobRequestBuilder()
        {
            return _jobRequestBuilderFactory.NewInstance();
        }

        public Task<FinalState> GetJobFinalStatus(string appId)
        {
            // FinalState is DataModel in YARN. For Azure Batch runtime, this is not supported.
            throw new NotImplementedException();
        }

        public void Submit(JobRequest jobRequest)
        {
            var configModule = AzureBatchRuntimeClientConfiguration.ConfigurationModule;
            string jobId = jobRequest.JobIdentifier;
            string azureBatchjobId = CreateAzureJobId(jobId);
            string commandLine = GetCommand(jobRequest.JobParameters);
            string jarPath = _jobJarMaker.CreateJobSubmissionJAR(jobRequest, azureBatchjobId);
            string destination = _azbatchFileNames.GetStorageJobFolder(azureBatchjobId);
            Uri blobUri = _azureStorageClient.UploadFile(destination, jarPath).Result;
            string sasToken = _azureStorageClient.CreateContainerSharedAccessSignature();
            _batchService.CreateJob(azureBatchjobId, blobUri, commandLine, sasToken);
        }

        private string GetCommand(JobParameters jobParameters)
        {
            var commandProviderConfigModule = AzureBatchCommandBuilderConfiguration.ConfigurationModule;
            if (jobParameters.JavaLogLevel == JavaLoggingSetting.Verbose)
            {
                commandProviderConfigModule = commandProviderConfigModule
                    .Set(AzureBatchCommandBuilderConfiguration.JavaDebugLogging, true.ToString().ToLowerInvariant());
            }

            if (jobParameters.StdoutFilePath.IsPresent())
            {
                commandProviderConfigModule = commandProviderConfigModule
                    .Set(AzureBatchCommandBuilderConfiguration.DriverStdoutFilePath, jobParameters.StdoutFilePath.Value);
            }

            if (jobParameters.StderrFilePath.IsPresent())
            {
                commandProviderConfigModule = commandProviderConfigModule
                    .Set(AzureBatchCommandBuilderConfiguration.DriverStderrFilePath, jobParameters.StderrFilePath.Value);
            }

            var azureBatchJobCommandBuilder = _injector.ForkInjector(commandProviderConfigModule.Build())
                .GetInstance<ICommandBuilder>();

            var command = azureBatchJobCommandBuilder.BuildDriverCommand(jobParameters.DriverMemoryInMB);

            return command;
        }

        public IJobSubmissionResult SubmitAndGetJobStatus(JobRequest jobRequest)
        {
            Submit(jobRequest);
            /// Azure Batch is not able to comminicate to client through driver end point. It behaves the same as Submit(JobRequest jobRequest).
            return null;
        }

        private string CreateAzureJobId(string jobId)
        {
            string guid = Guid.NewGuid().ToString();
            string jobNameShort = jobId.Length + 1 + guid.Length < AzureBatchMaxCharsJobName ?
                jobId : jobId.Substring(0, AzureBatchMaxCharsJobName - guid.Length - 1);
            return jobNameShort + "-" + guid;
        }
    }
}
