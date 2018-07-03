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
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Org.Apache.REEF.Client.AzureBatch.Parameters;
using Org.Apache.REEF.Client.AzureBatch.Util;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using BatchSharedKeyCredential = Microsoft.Azure.Batch.Auth.BatchSharedKeyCredentials;

namespace Org.Apache.REEF.Client.DotNet.AzureBatch
{
    internal sealed class AzureBatchService : IDisposable
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(AzureBatchService));
        private static readonly TimeSpan RetryDeltaBackOff = TimeSpan.FromSeconds(5);
        private const string AzureStorageContainerSasToken = "AZURE_STORAGE_CONTAINER_SAS_TOKEN_ENV";
        private const int MaxRetries = 3;

        public BatchSharedKeyCredential Credentials { get; private set; }
        public string PoolId { get; private set; }

        private BatchClient Client { get; set; }
        private ContainerRegistryProvider ContainerRegistryProvider { get; set; }
        private ITcpPortProvider PortProvider { get; set; }
        private ICommandBuilder CommandBuilder { get; set; }

        private bool areContainersEnabled;
        private bool disposed;

        [Inject]
        public AzureBatchService(
            ITcpPortProvider portProvider,
            ContainerRegistryProvider containerRegistryProvider,
            ICommandBuilder commandBuilder,
            [Parameter(typeof(AzureBatchAccountUri))] string azureBatchAccountUri,
            [Parameter(typeof(AzureBatchAccountName))] string azureBatchAccountName,
            [Parameter(typeof(AzureBatchAccountKey))] string azureBatchAccountKey,
            [Parameter(typeof(AzureBatchPoolId))] string azureBatchPoolId)
        {
            BatchSharedKeyCredential credentials = new BatchSharedKeyCredential(azureBatchAccountUri, azureBatchAccountName, azureBatchAccountKey);

            this.Client = BatchClient.Open(credentials);
            this.Credentials = credentials;
            this.PoolId = azureBatchPoolId;
            this.ContainerRegistryProvider = containerRegistryProvider;
            this.PortProvider = portProvider;
            this.Client.CustomBehaviors.Add(new RetryPolicyProvider(new ExponentialRetry(RetryDeltaBackOff, MaxRetries)));
            this.areContainersEnabled = this.ContainerRegistryProvider.IsValid();
            this.CommandBuilder = commandBuilder;
        }

        /// <summary>
        /// Dispose of this object and all members
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~AzureBatchService()
        {
            this.Dispose(false);
        }

        /// <summary>
        /// Disposes of this object
        /// </summary>
        private void Dispose(bool disposing)
        {
            if (this.disposed)
            {
                return;
            }

            if (disposing)
            {
                this.Client.Dispose();
            }

            this.disposed = true;
        }

        #region Job related operations

        public void CreateJob(string jobId, Uri resourceFile, string commandLine, string storageContainerSAS)
        {
            CloudJob unboundJob = this.Client.JobOperations.CreateJob();
            unboundJob.Id = jobId;
            unboundJob.PoolInformation = new PoolInformation() { PoolId = this.PoolId };
            unboundJob.JobPreparationTask = CreateJobPreparationTask();
            unboundJob.JobManagerTask = new JobManagerTask()
            {
                Id = jobId,
                CommandLine = commandLine,
                RunExclusive = false,

                ResourceFiles = resourceFile != null
                    ? new List<ResourceFile>() { new ResourceFile(resourceFile.AbsoluteUri, AzureBatchFileNames.GetTaskJarFileName()) }
                    : new List<ResourceFile>(),

                EnvironmentSettings = new List<EnvironmentSetting> { new EnvironmentSetting(AzureStorageContainerSasToken, storageContainerSAS) },

                // This setting will signal Batch to generate an access token and pass it
                // to the Job Manager Task (aka the Driver) as an environment variable.
                // For more info, see
                // https://docs.microsoft.com/en-us/dotnet/api/microsoft.azure.batch.cloudtask.authenticationtokensettings
                AuthenticationTokenSettings = new AuthenticationTokenSettings() { Access = AccessScope.Job },
                ContainerSettings = CreateTaskContainerSettings(),
                UserIdentity = new UserIdentity(autoUserSpecification: new AutoUserSpecification(elevationLevel: ElevationLevel.Admin)),
            };

            unboundJob.Commit();

            LOGGER.Log(Level.Info, "Submitted job {0}, commandLine {1} ", jobId, commandLine);
        }

        private JobPreparationTask CreateJobPreparationTask()
        {
            if (!this.areContainersEnabled)
            {
                return null;
            }

            return new JobPreparationTask() { Id = "CaptureHostIpAddress", CommandLine = this.CommandBuilder.CaptureIpAddressCommandLine() };
        }

        private TaskContainerSettings CreateTaskContainerSettings()
        {
            if (!this.areContainersEnabled)
            {
                return null;
            }

            string portMappings = this.PortProvider
                .Select(x => x.ToString())
                .Aggregate(seed: string.Empty, func: (aggregator, port) => $"{aggregator}  -p {port}:{port}");

            return new TaskContainerSettings(
                imageName: this.ContainerRegistryProvider.ContainerImageName,
                containerRunOptions: $"-dit --env HOST_IP_ADDR_PATH={this.CommandBuilder.GetIpAddressFilePath()} {portMappings}",
                registry: this.ContainerRegistryProvider.GetContainerRegistry());
        }

        public CloudJob GetJob(string jobId, DetailLevel detailLevel)
        {
            using (Task<CloudJob> getJobTask = this.GetJobAsync(jobId, detailLevel))
            {
                getJobTask.Wait();
                return getJobTask.Result;
            }
        }

        public Task<CloudJob> GetJobAsync(string jobId, DetailLevel detailLevel)
        {
            return this.Client.JobOperations.GetJobAsync(jobId, detailLevel);
        }

        public CloudTask GetJobManagerTaskFromJobId(string jobId)
        {
            string driverTaskId = this.Client.JobOperations.GetJob(jobId).JobManagerTask.Id;
            return this.Client.JobOperations.GetTask(jobId, driverTaskId);
        }

        public ComputeNode GetComputeNodeFromNodeId(string nodeId)
        {
            return this.Client.PoolOperations.GetComputeNode(this.PoolId, nodeId);
        }

        #endregion
    }
}