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
        internal string SerializeAppFile(IJobSubmission jobSubmission, IInjector paramInjector, string driverFolderPath)
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
                driverMemory = jobSubmission.DriverMemory,
                driverRecoveryTimeout = paramInjector.GetNamedInstance<DriverBridgeConfigurationOptions.DriverRestartEvaluatorRecoverySeconds, int>()
            };

            var avroYarnClusterAppSubmissionParameters = new AvroYarnClusterAppSubmissionParameters
            {
                yarnAppSubmissionParameters = avroYarnAppSubmissionParameters,
                maxApplicationSubmissions = paramInjector.GetNamedInstance<DriverBridgeConfigurationOptions.MaxApplicationSubmissions, int>()
            };

            var submissionArgsFilePath = Path.Combine(driverFolderPath, _fileNames.GetSubmissionAppParametersFile());
            using (var argsFileStream = new FileStream(submissionArgsFilePath, FileMode.CreateNew))
            {
                var serializedArgs = AvroJsonSerializer<AvroYarnClusterAppSubmissionParameters>.ToBytes(avroYarnClusterAppSubmissionParameters);
                argsFileStream.Write(serializedArgs, 0, serializedArgs.Length);
            }

            return submissionArgsFilePath;
        }

        /// <summary>
        /// Serializes the job parameters to job-submission-params.json.
        /// </summary>
        internal string SerializeJobFile(IJobSubmission jobSubmission, string driverFolderPath)
        {
            var avroJobSubmissionParameters = new AvroJobSubmissionParameters
            {
                jobId = jobSubmission.JobIdentifier,
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
                yarnJobSubmissionParameters = avroYarnJobSubmissionParameters
            };

            var submissionArgsFilePath = Path.Combine(driverFolderPath, _fileNames.GetSubmissionJobParametersFile());
            using (var argsFileStream = new FileStream(submissionArgsFilePath, FileMode.CreateNew))
            {
                var serializedArgs = AvroJsonSerializer<AvroYarnClusterJobSubmissionParameters>.ToBytes(avroYarnClusterJobSubmissionParameters);
                argsFileStream.Write(serializedArgs, 0, serializedArgs.Length);
            }

            return submissionArgsFilePath;
        }
    }
}
