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
using System.Linq;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Utilities.Logging;
using BatchSharedKeyCredential = Microsoft.Azure.Batch.Auth.BatchSharedKeyCredentials;

namespace Org.Apache.REEF.Client.AzureBatch
{
    internal class AzureBatchJobSubmissionResult : JobSubmissionResult
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(AzureBatchJobSubmissionResult));
        private const string AzureBatchTaskWorkDirectory = "wd";
        private readonly BatchClient _client;
        private readonly string _azureBatchPoolId;
        private readonly string _jobId;
        private readonly int _numberOfRetries;
        private readonly int _retryInterval;

        internal AzureBatchJobSubmissionResult(IREEFClient reefClient,
            string filePath,
            string jobId,
            int numberOfRetries,
            int retryInterval,
            string azureBatchPoolId,
            string azureBatchUrl,
            string azureBatchAccountName,
            string azureBatchAccountKey) : base(reefClient, filePath, numberOfRetries, retryInterval)
        {
            _jobId = jobId;
            _azureBatchPoolId = azureBatchPoolId;
            _numberOfRetries = numberOfRetries;
            _retryInterval = retryInterval;
            BatchSharedKeyCredential credentials = new BatchSharedKeyCredential(azureBatchUrl, azureBatchAccountName, azureBatchAccountKey);
            _client = BatchClient.Open(credentials);
        }

        protected override string GetDriverUrl(string filepath)
        {
            var policy = new RetryPolicy<AllErrorsTransientStrategy>(_numberOfRetries, TimeSpan.FromMilliseconds(_retryInterval));
            return policy.ExecuteAction(() => GetDriverUrlInternal(filepath));
        }

        private string GetDriverUrlInternal(string filepath)
        {
            string driverTaskId = _client.JobOperations.GetJob(_jobId).JobManagerTask.Id;
            CloudTask driverTask = _client.JobOperations.GetTask(_jobId, driverTaskId);

            NodeFile httpEndPointFile;
            try
            {
                httpEndPointFile = driverTask.GetNodeFile(Path.Combine(AzureBatchTaskWorkDirectory, filepath));
            }
            catch (BatchException e)
            {
                throw new InvalidOperationException("driver http endpoint file is not ready.", e);
            }

            string driverHostData = httpEndPointFile.ReadAsString();
            string driverHost;
            if (driverHostData.Length > 0)
            {
                //// Remove last charactor '\n'
                driverHost = driverHostData.Substring(0, driverHostData.Length - 1);
            }
            else
            {
                LOGGER.Log(Level.Warning, "unable to get driver http endpoint. The format in remote file is not correct.");
                return null;
            }

            //// Get port
            string[] driverIpAndPorts = driverHost.Split(':');
            string backendPort;
            if (driverIpAndPorts.Length > 1)
            {
                backendPort = driverIpAndPorts[1];
            }
            else
            {
                LOGGER.Log(Level.Warning, "unable to get driver http endpoint port. The format in remote file is not correct.");
                return null;
            }

            //// Get public Ip
            string publicIp = "0.0.0.0";
            int frontEndPort = 0;
            string driverNodeId = driverTask.ComputeNodeInformation.ComputeNodeId;
            ComputeNode driverNode = _client.PoolOperations.GetComputeNode(_azureBatchPoolId, driverNodeId);
            IReadOnlyList<InboundEndpoint> inboundEndpoints = driverNode.EndpointConfiguration.InboundEndpoints;
            InboundEndpoint endpoint = inboundEndpoints.FirstOrDefault(s => s.BackendPort.ToString().Equals(backendPort));

            if (endpoint != null)
            {
                publicIp = endpoint.PublicIPAddress;
                frontEndPort = endpoint.FrontendPort;
            }

            return "http://" + publicIp + ':' + frontEndPort + '/';
        }
    }
}
