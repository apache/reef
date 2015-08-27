/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.IO;
using System.Linq;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.YARN
{
    internal sealed class YARNClient : IREEFClient
    {
        /// <summary>
        /// The class name that contains the Java counterpart for this client.
        /// </summary>
        private const string JavaClassName = "org.apache.reef.bridge.client.YarnJobSubmissionClient";

        private static readonly Logger Logger = Logger.GetLogger(typeof(YARNClient));
        private readonly DriverFolderPreparationHelper _driverFolderPreparationHelper;
        private readonly JavaClientLauncher _javaClientLauncher;

        [Inject]
        internal YARNClient(JavaClientLauncher javaClientLauncher,
            DriverFolderPreparationHelper driverFolderPreparationHelper,
            YarnCommandLineEnvironment yarn)
        {
            _javaClientLauncher = javaClientLauncher;
            _javaClientLauncher.AddToClassPath(yarn.GetYarnClasspathList());
            _driverFolderPreparationHelper = driverFolderPreparationHelper;
        }

        public void Submit(IJobSubmission jobSubmission)
        {
            // Prepare the job submission folder
            var driverFolderPath = CreateDriverFolder(jobSubmission.JobIdentifier);
            Logger.Log(Level.Info, "Preparing driver folder in " + driverFolderPath);

            _driverFolderPreparationHelper.PrepareDriverFolder(jobSubmission, driverFolderPath);

            //TODO: Remove this when we have a generalized way to pass config to java
            var javaParams = TangFactory.GetTang()
                .NewInjector(jobSubmission.DriverConfigurations.ToArray())
                .GetInstance<ClrClient2JavaClientCuratedParameters>();

            // Submit the driver
            _javaClientLauncher.Launch(JavaClassName, driverFolderPath, jobSubmission.JobIdentifier,
                jobSubmission.DriverMemory.ToString(),
                javaParams.TcpPortRangeStart.ToString(),
                javaParams.TcpPortRangeCount.ToString(),
                javaParams.TcpPortRangeTryCount.ToString(),
                javaParams.MaxApplicationSubmissions.ToString(),
                javaParams.DriverRestartEvaluatorRecoverySeconds.ToString()
                );
            Logger.Log(Level.Info, "Submitted the Driver for execution.");
        }

        /// <summary>
        /// Creates the temporary directory to hold the job submission.
        /// </summary>
        /// <returns>The path to the folder created.</returns>
        private string CreateDriverFolder(string jobId)
        {
            var timestamp = DateTime.Now.ToString("yyyyMMddHHmmssfff");
            return Path.GetFullPath(Path.Combine(Path.GetTempPath(), string.Join("-", "reef", jobId, timestamp)));
        }
    }
}