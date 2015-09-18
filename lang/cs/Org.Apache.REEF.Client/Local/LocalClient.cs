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
using System.Threading.Tasks;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.Local.Parameters;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.Local
{
    /// <summary>
    /// An implementation of the REEF interface using an external Java program
    /// </summary>
    public sealed class LocalClient : IREEFClient
    {
        /// <summary>
        /// The class name that contains the Java counterpart for this client.
        /// </summary>
        private const string JavaClassName = "org.apache.reef.bridge.client.LocalClient";

        /// <summary>
        /// The name of the folder in the job's working directory that houses the driver.
        /// </summary>
        private const string DriverFolderName = "driver";

        private static readonly Logger Logger = Logger.GetLogger(typeof(LocalClient));
        private readonly DriverFolderPreparationHelper _driverFolderPreparationHelper;
        private readonly JavaClientLauncher _javaClientLauncher;
        private readonly int _numberOfEvaluators;
        private readonly string _runtimeFolder;
        private string _driverUrl;
        private REEFFileNames _fileNames;

        [Inject]
        private LocalClient(DriverFolderPreparationHelper driverFolderPreparationHelper,
            [Parameter(typeof(LocalRuntimeDirectory))] string runtimeFolder,
            [Parameter(typeof(NumberOfEvaluators))] int numberOfEvaluators,
            JavaClientLauncher javaClientLauncher,
            REEFFileNames fileNames)
        {
            _driverFolderPreparationHelper = driverFolderPreparationHelper;
            _runtimeFolder = runtimeFolder;
            _numberOfEvaluators = numberOfEvaluators;
            _javaClientLauncher = javaClientLauncher;
            _fileNames = fileNames;
        }

        /// <summary>
        /// Uses Path.GetTempPath() as the runtime execution folder.
        /// </summary>
        /// <param name="driverFolderPreparationHelper"></param>
        /// <param name="numberOfEvaluators"></param>
        /// <param name="javaClientLauncher"></param>
        /// <param name="fileNames"></param>
        [Inject]
        private LocalClient(
            DriverFolderPreparationHelper driverFolderPreparationHelper,
            [Parameter(typeof(NumberOfEvaluators))] int numberOfEvaluators,
            JavaClientLauncher javaClientLauncher,
            REEFFileNames fileNames)
            : this(driverFolderPreparationHelper, Path.GetTempPath(), numberOfEvaluators, javaClientLauncher, fileNames)
        {
            // Intentionally left blank.
        }

        public void Submit(IJobSubmission jobSubmission)
        {
            // Prepare the job submission folder
            var jobFolder = CreateJobFolder(jobSubmission.JobIdentifier);
            var driverFolder = Path.Combine(jobFolder, DriverFolderName);
            Logger.Log(Level.Info, "Preparing driver folder in " + driverFolder);

            _driverFolderPreparationHelper.PrepareDriverFolder(jobSubmission, driverFolder);

            //TODO: Remove this when we have a generalized way to pass config to java
            var javaParams = TangFactory.GetTang()
                .NewInjector(jobSubmission.DriverConfigurations.ToArray())
                .GetInstance<ClrClient2JavaClientCuratedParameters>();

            _javaClientLauncher.Launch(JavaClassName, driverFolder, jobSubmission.JobIdentifier,
                _numberOfEvaluators.ToString(),
                javaParams.TcpPortRangeStart.ToString(),
                javaParams.TcpPortRangeCount.ToString(),
                javaParams.TcpPortRangeTryCount.ToString()
                );
            Logger.Log(Level.Info, "Submitted the Driver for execution.");
        }

        public IDriverHttpEndpoint SubmitAndGetDriverUrl(IJobSubmission jobSubmission)
        {
            // Prepare the job submission folder
            var jobFolder = CreateJobFolder(jobSubmission.JobIdentifier);
            var driverFolder = Path.Combine(jobFolder, DriverFolderName);
            Logger.Log(Level.Info, "Preparing driver folder in " + driverFolder);

            _driverFolderPreparationHelper.PrepareDriverFolder(jobSubmission, driverFolder);

            //TODO: Remove this when we have a generalized way to pass config to java
            var javaParams = TangFactory.GetTang()
                .NewInjector(jobSubmission.DriverConfigurations.ToArray())
                .GetInstance<ClrClient2JavaClientCuratedParameters>();

            Task.Run(() =>
            _javaClientLauncher.Launch(JavaClassName, driverFolder, jobSubmission.JobIdentifier,
                _numberOfEvaluators.ToString(),
                javaParams.TcpPortRangeStart.ToString(),
                javaParams.TcpPortRangeCount.ToString(),
                javaParams.TcpPortRangeTryCount.ToString()
                ));

            var fileName = Path.Combine(driverFolder, _fileNames.DriverHttpEndpoint);
            HttpClientHelper helper = new HttpClientHelper();
            _driverUrl = helper.GetDriverUrlForLocalRuntime(fileName);

            Logger.Log(Level.Info, "Submitted the Driver for execution. Returned driverUrl is: " + _driverUrl);
            return helper;
        }

        public string DriverUrl
        {
            get { return _driverUrl; }
        }

        /// <summary>
        /// Creates the temporary directory to hold the job submission.
        /// </summary>
        /// <returns></returns>
        private string CreateJobFolder(string jobId)
        {
            var timestamp = DateTime.Now.ToString("yyyyMMddHHmmssfff");
            return Path.Combine(_runtimeFolder, string.Join("-", "reef", jobId, timestamp));
        }
    }
}