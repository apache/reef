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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.Yarn.RestClient;
using Org.Apache.REEF.Client.YARN;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.Yarn
{
    internal sealed class YarnREEFClient : IREEFClient
    {
        /// <summary>
        /// The class name that contains the Java counterpart for this client.
        /// </summary>
        private const string JavaClassName = "org.apache.reef.bridge.client.YarnJobSubmissionClient";

        private static readonly Logger Logger = Logger.GetLogger(typeof(YarnREEFClient));
        private readonly DriverFolderPreparationHelper _driverFolderPreparationHelper;
        private readonly IJavaClientLauncher _javaClientLauncher;
        private readonly REEFFileNames _fileNames;
        private readonly IYarnRMClient _yarnClient;
        private readonly YarnREEFParamSerializer _paramSerializer;

        [Inject]
        internal YarnREEFClient(IJavaClientLauncher javaClientLauncher,
            DriverFolderPreparationHelper driverFolderPreparationHelper,
            REEFFileNames fileNames,
            YarnCommandLineEnvironment yarn,
            IYarnRMClient yarnClient,
            YarnREEFParamSerializer paramSerializer)
        {
            _javaClientLauncher = javaClientLauncher;
            _javaClientLauncher.AddToClassPath(yarn.GetYarnClasspathList());
            _driverFolderPreparationHelper = driverFolderPreparationHelper;
            _fileNames = fileNames;
            _yarnClient = yarnClient;
            _paramSerializer = paramSerializer;
        }

        public void Submit(JobRequest jobRequest)
        {
            // Prepare the job submission folder
            var driverFolderPath = CreateDriverFolder(jobRequest.JobIdentifier);
            Logger.Log(Level.Info, "Preparing driver folder in " + driverFolderPath);

            Launch(jobRequest, driverFolderPath);
        }

        public IJobSubmissionResult SubmitAndGetJobStatus(JobRequest jobRequest)
        {
            // Prepare the job submission folder
            var driverFolderPath = CreateDriverFolder(jobRequest.JobIdentifier);
            Logger.Log(Level.Info, "Preparing driver folder in " + driverFolderPath);

            Launch(jobRequest, driverFolderPath);

            var pointerFileName = Path.Combine(driverFolderPath, _fileNames.DriverHttpEndpoint);
            var jobSubmitionResultImpl = new YarnJobSubmissionResult(this, pointerFileName);

            var msg = string.Format(CultureInfo.CurrentCulture,
                "Submitted the Driver for execution. Returned driverUrl is: {0}, appId is {1}.",
                jobSubmitionResultImpl.DriverUrl, jobSubmitionResultImpl.AppId);
            Logger.Log(Level.Info, msg);

            return jobSubmitionResultImpl;
        }

        /// <summary>
        /// Pull Job status from Yarn for the given appId
        /// </summary>
        /// <returns></returns>
        public async Task<FinalState> GetJobFinalStatus(string appId)
        {
            var application = await _yarnClient.GetApplicationAsync(appId);

            var msg = string.Format("application status {0}, Progress: {1}, trackingUri: {2}, Name: {3}, ApplicationId: {4}, State {5}.",
                application.FinalStatus, application.Progress, application.TrackingUI, application.Name, application.Id, application.State);
            Logger.Log(Level.Verbose, msg);

            return application.FinalStatus;
        }

        private void Launch(JobRequest jobRequest, string driverFolderPath)
        {
            _driverFolderPreparationHelper.PrepareDriverFolder(jobRequest.AppParameters, driverFolderPath);

            // TODO: Remove this when we have a generalized way to pass config to java
            var paramInjector = TangFactory.GetTang().NewInjector(jobRequest.DriverConfigurations.ToArray());
            var submissionJobArgsFilePath = _paramSerializer.SerializeJobFile(jobRequest.JobParameters, paramInjector, driverFolderPath);
            var submissionAppArgsFilePath = _paramSerializer.SerializeAppFile(jobRequest.AppParameters, paramInjector, driverFolderPath);

            // Submit the driver
            _javaClientLauncher.LaunchAsync(JavaClassName, submissionJobArgsFilePath, submissionAppArgsFilePath)
                .GetAwaiter()
                .GetResult();
            Logger.Log(Level.Info, "Submitted the Driver for execution." + jobRequest.JobIdentifier);
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