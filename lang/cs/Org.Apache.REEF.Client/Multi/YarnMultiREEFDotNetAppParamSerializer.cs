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

using System.Collections.Generic;
using System.IO;
using System.Linq;

using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Avro;
using Org.Apache.REEF.Client.Avro.Local;
using Org.Apache.REEF.Client.Avro.Multi;
using Org.Apache.REEF.Client.Avro.YARN;
using Org.Apache.REEF.Client.Local.Parameters;
using Org.Apache.REEF.Client.Multi.Parameters;
using Org.Apache.REEF.Client.YARN;
using Org.Apache.REEF.Common.Avro;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Common.Runtime;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Wake.Remote.Parameters;

namespace Org.Apache.REEF.Client.Multi
{
    /// <summary>
    /// Application Parameters serializer for multi runtime environment on yarn
    /// </summary>
    internal sealed class YarnMultiREEFDotNetAppParamSerializer : IYarnREEFDotNetAppParamsSerializer
    {
        private readonly REEFFileNames _fileNames;

        [Inject]
        private YarnMultiREEFDotNetAppParamSerializer(REEFFileNames fileNames)
        {
            _fileNames = fileNames;
        }

        /// <summary>
        /// Serializes the application parameters to reef/local/app-submission-params.json.
        /// </summary>
        public void SerializeAppFile(AppParameters appParameters, IInjector paramInjector, string localDriverFolderPath)
        {
            var serializedArgs = SerializeAppArgsToBytes(appParameters, paramInjector, localDriverFolderPath);

            var submissionAppArgsFilePath = Path.Combine(
                localDriverFolderPath, _fileNames.GetLocalFolderPath(), _fileNames.GetAppSubmissionParametersFile());

            using (var jobArgsFileStream = new FileStream(submissionAppArgsFilePath, FileMode.CreateNew))
            {
                jobArgsFileStream.Write(serializedArgs, 0, serializedArgs.Length);
            }
        }

        private byte[] SerializeAppArgsToBytes(AppParameters appParameters, IInjector paramInjector, string localDriverFolderPath)
        {
            var multiAppSubmissionParameters = new AvroMultiRuntimeAppSubmissionParameters();
            multiAppSubmissionParameters.defaultRuntimeName = paramInjector.GetNamedInstance<DefaultRuntime, string>();
            
            multiAppSubmissionParameters.runtimes = paramInjector.GetNamedInstance<DefinedRuntimes, ISet<string>>().ToList();
            var avroAppSubmissionParameters = new AvroAppSubmissionParameters
                                                  {
                                                      tcpBeginPort = paramInjector.GetNamedInstance<TcpPortRangeStart, int>(),
                                                      tcpRangeCount = paramInjector.GetNamedInstance<TcpPortRangeCount, int>(),
                                                      tcpTryCount = paramInjector.GetNamedInstance<TcpPortRangeTryCount, int>()
                                                  };
            multiAppSubmissionParameters.sharedAppSubmissionParameters = avroAppSubmissionParameters;

            if (multiAppSubmissionParameters.runtimes.Contains(RuntimeName.Local.ToString()))
            {
                var avroLocalAppSubmissionParameters = new AvroLocalAppSubmissionParameters
                {
                    sharedAppSubmissionParameters = avroAppSubmissionParameters,
                    maxNumberOfConcurrentEvaluators = paramInjector.GetNamedInstance<NumberOfEvaluators, int>()
                };

                multiAppSubmissionParameters.localRuntimeAppParameters = avroLocalAppSubmissionParameters;
            }

            if (multiAppSubmissionParameters.runtimes.Contains(RuntimeName.Yarn.ToString()))
            {
                var avroYarnAppSubmissionParameters = new AvroYarnAppSubmissionParameters
                {
                    sharedAppSubmissionParameters = avroAppSubmissionParameters,
                    driverRecoveryTimeout =
                        paramInjector.GetNamedInstance<DriverBridgeConfigurationOptions.DriverRestartEvaluatorRecoverySeconds, int>()
                };

                multiAppSubmissionParameters.yarnRuntimeAppParameters = avroYarnAppSubmissionParameters;
            }

            return AvroJsonSerializer<AvroMultiRuntimeAppSubmissionParameters>.ToBytes(multiAppSubmissionParameters);
        }
    }
}