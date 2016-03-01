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

using System.IO;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Avro;
using Org.Apache.REEF.Client.Avro.YARN;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.YARN.Parameters;
using Org.Apache.REEF.Common.Avro;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Wake.Remote.Parameters;

namespace Org.Apache.REEF.Client.YARN
{
    /// <summary>
    /// Job/application parameter file serializer for <see cref="YarnREEFClient"/>.
    /// </summary>
    internal sealed class YarnREEFParamSerializer
    {
        private readonly REEFFileNames _fileNames;
        private readonly string _securityTokenKind;
        private readonly string _securityTokenService;
        private readonly string _jobSubmissionPrefix;

        [Inject]
        private YarnREEFParamSerializer(
            REEFFileNames fileNames,
            [Parameter(typeof(SecurityTokenKindParameter))] string securityTokenKind,
            [Parameter(typeof(SecurityTokenServiceParameter))] string securityTokenService,
            [Parameter(typeof(JobSubmissionDirectoryPrefixParameter))] string jobSubmissionPrefix)
        {
            _fileNames = fileNames;
            _jobSubmissionPrefix = jobSubmissionPrefix;
            _securityTokenKind = securityTokenKind;
            _securityTokenService = securityTokenService;
        }

        /// <summary>
        /// Serializes the application parameters to reef/local/app-submission-params.json.
        /// </summary>
        internal string SerializeAppFile(AppParameters appParameters, IInjector paramInjector, string driverFolderPath)
        {
            var serializedArgs = SerializeAppArgsToBytes(appParameters, paramInjector);

            var submissionArgsFilePath = Path.Combine(driverFolderPath, _fileNames.GetAppSubmissionParametersFile());
            using (var argsFileStream = new FileStream(submissionArgsFilePath, FileMode.CreateNew))
            {
                argsFileStream.Write(serializedArgs, 0, serializedArgs.Length);
            }

            return submissionArgsFilePath;
        }

        internal byte[] SerializeAppArgsToBytes(AppParameters appParameters, IInjector paramInjector)
        {
            var avroAppSubmissionParameters = new AvroAppSubmissionParameters
            {
                tcpBeginPort = paramInjector.GetNamedInstance<TcpPortRangeStart, int>(),
                tcpRangeCount = paramInjector.GetNamedInstance<TcpPortRangeCount, int>(),
                tcpTryCount = paramInjector.GetNamedInstance<TcpPortRangeTryCount, int>()
            };

            var avroYarnAppSubmissionParameters = new AvroYarnAppSubmissionParameters
            {
                sharedAppSubmissionParameters = avroAppSubmissionParameters,
                driverRecoveryTimeout = paramInjector.GetNamedInstance<DriverBridgeConfigurationOptions.DriverRestartEvaluatorRecoverySeconds, int>()
            };

            return AvroJsonSerializer<AvroYarnAppSubmissionParameters>.ToBytes(avroYarnAppSubmissionParameters);
        }

        /// <summary>
        /// Serializes the job parameters to job-submission-params.json.
        /// </summary>
        internal string SerializeJobFile(JobParameters jobParameters, IInjector paramInjector, string driverFolderPath)
        {
            var serializedArgs = SerializeJobArgsToBytes(jobParameters, driverFolderPath);

            var submissionArgsFilePath = Path.Combine(driverFolderPath, _fileNames.GetJobSubmissionParametersFile());
            using (var argsFileStream = new FileStream(submissionArgsFilePath, FileMode.CreateNew))
            {
                argsFileStream.Write(serializedArgs, 0, serializedArgs.Length);
            }

            return submissionArgsFilePath;
        }

        internal byte[] SerializeJobArgsToBytes(JobParameters jobParameters, string driverFolderPath)
        {
            var avroJobSubmissionParameters = new AvroJobSubmissionParameters
            {
                jobId = jobParameters.JobIdentifier,
                jobSubmissionFolder = driverFolderPath
            };

            var avroYarnJobSubmissionParameters = new AvroYarnJobSubmissionParameters
            {
                jobSubmissionDirectoryPrefix = _jobSubmissionPrefix,
                sharedJobSubmissionParameters = avroJobSubmissionParameters
            };

            var avroYarnClusterJobSubmissionParameters = new AvroYarnClusterJobSubmissionParameters
            {
                securityTokenKind = _securityTokenKind,
                securityTokenService = _securityTokenService,
                yarnJobSubmissionParameters = avroYarnJobSubmissionParameters,
                driverMemory = jobParameters.DriverMemoryInMB,
                maxApplicationSubmissions = jobParameters.MaxApplicationSubmissions,
                driverStdoutFilePath = string.IsNullOrWhiteSpace(jobParameters.StdoutFilePath.Value) ?
                    _fileNames.GetDefaultYarnDriverStdoutFilePath() : jobParameters.StdoutFilePath.Value,
                driverStderrFilePath = string.IsNullOrWhiteSpace(jobParameters.StderrFilePath.Value) ? 
                    _fileNames.GetDefaultYarnDriverStderrFilePath() : jobParameters.StderrFilePath.Value
            };

            return AvroJsonSerializer<AvroYarnClusterJobSubmissionParameters>.ToBytes(avroYarnClusterJobSubmissionParameters);
        }
    }
}
