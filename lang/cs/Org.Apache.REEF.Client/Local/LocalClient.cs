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
using Org.Apache.REEF.Client.Avro;
using Org.Apache.REEF.Client.Avro.Local;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.Local.Parameters;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Common.Avro;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.AsyncUtils;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Wake.Remote.Parameters;

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
        private readonly IJavaClientLauncher _javaClientLauncher;
        private readonly int _maxNumberOfConcurrentEvaluators;
        private readonly string _runtimeFolder;
        private readonly REEFFileNames _fileNames;
        private readonly IConfiguration _localConfigurationOnDriver;

        [Inject]
        private LocalClient(DriverFolderPreparationHelper driverFolderPreparationHelper,
            [Parameter(typeof(LocalRuntimeDirectory))] string runtimeFolder,
            [Parameter(typeof(NumberOfEvaluators))] int maxNumberOfConcurrentEvaluators,
            IJavaClientLauncher javaClientLauncher,
            REEFFileNames fileNames)
        {
            _driverFolderPreparationHelper = driverFolderPreparationHelper;
            _runtimeFolder = runtimeFolder;
            _maxNumberOfConcurrentEvaluators = maxNumberOfConcurrentEvaluators;
            _javaClientLauncher = javaClientLauncher;
            _fileNames = fileNames;
            _localConfigurationOnDriver = TangFactory.GetTang().NewConfigurationBuilder().BindImplementation(GenericType<ILocalAddressProvider>.Class, GenericType<LoopbackLocalAddressProvider>.Class).Build();
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
            IJavaClientLauncher javaClientLauncher,
            REEFFileNames fileNames)
            : this(driverFolderPreparationHelper, Path.GetTempPath(), numberOfEvaluators, javaClientLauncher, fileNames)
        {
            // Intentionally left blank.
        }

        private string CreateBootstrapAvroJobConfig(JobParameters jobParameters, string driverFolder)
        {
            var bootstrapJobArgs = new AvroJobSubmissionParameters
            {
                jobSubmissionFolder = driverFolder,
                jobId = jobParameters.JobIdentifier,
            };

            var bootstrapLocalJobArgs = new AvroLocalJobSubmissionParameters
            {
                sharedJobSubmissionParameters = bootstrapJobArgs,
                driverStdoutFilePath = jobParameters.StdoutFilePath.IsPresent() ? jobParameters.StdoutFilePath.Value : _fileNames.GetDriverStdoutFileName(),
                driverStderrFilePath = jobParameters.StderrFilePath.IsPresent() ? jobParameters.StderrFilePath.Value : _fileNames.GetDriverStderrFileName()
            };

            var submissionArgsFilePath = Path.Combine(driverFolder, _fileNames.GetJobSubmissionParametersFile());
            using (var argsFileStream = new FileStream(submissionArgsFilePath, FileMode.CreateNew))
            {
                var serializedArgs = AvroJsonSerializer<AvroLocalJobSubmissionParameters>.ToBytes(bootstrapLocalJobArgs);
                argsFileStream.Write(serializedArgs, 0, serializedArgs.Length);
            }

            return submissionArgsFilePath;
        }

        private string CreateBootstrapAvroAppConfig(AppParameters appParameters, string driverFolder)
        {
            var paramInjector = TangFactory.GetTang().NewInjector(appParameters.DriverConfigurations.ToArray());

            var bootstrapAppArgs = new AvroAppSubmissionParameters
            {
                tcpBeginPort = paramInjector.GetNamedInstance<TcpPortRangeStart, int>(),
                tcpRangeCount = paramInjector.GetNamedInstance<TcpPortRangeCount, int>(),
                tcpTryCount = paramInjector.GetNamedInstance<TcpPortRangeTryCount, int>(),
            };

            var avroLocalBootstrapAppArgs = new AvroLocalAppSubmissionParameters
            {
                sharedAppSubmissionParameters = bootstrapAppArgs,
                maxNumberOfConcurrentEvaluators = _maxNumberOfConcurrentEvaluators
            };

            var submissionArgsFilePath = Path.Combine(driverFolder, _fileNames.GetAppSubmissionParametersFile());
            using (var argsFileStream = new FileStream(submissionArgsFilePath, FileMode.CreateNew))
            {
                var serializedArgs = AvroJsonSerializer<AvroLocalAppSubmissionParameters>.ToBytes(avroLocalBootstrapAppArgs);
                argsFileStream.Write(serializedArgs, 0, serializedArgs.Length);
            }

            return submissionArgsFilePath;
        }

        private string PrepareDriverFolder(JobRequest jobRequest)
        {
            // Prepare the job submission folder
            var jobFolder = CreateJobFolder(jobRequest.JobIdentifier);
            var driverFolder = Path.Combine(jobFolder, DriverFolderName);
            Logger.Log(Level.Info, "Preparing driver folder in " + driverFolder);

            _driverFolderPreparationHelper.PrepareDriverFolder(jobRequest.AppParameters, driverFolder);

            return driverFolder;
        }

        public void Submit(JobRequest jobRequest)
        {
            var driverFolder = PrepareDriverFolder(jobRequest);
            var submissionJobArgsFilePath = CreateBootstrapAvroJobConfig(jobRequest.JobParameters, driverFolder);
            var submissionAppArgsFilePath = CreateBootstrapAvroAppConfig(jobRequest.AppParameters, driverFolder);
            _javaClientLauncher.LaunchAsync(jobRequest.JavaLogLevel, JavaClassName, submissionJobArgsFilePath, submissionAppArgsFilePath)
                .GetAwaiter().GetResult();
            Logger.Log(Level.Info, "Submitted the Driver for execution.");
        }

        public IJobSubmissionResult SubmitAndGetJobStatus(JobRequest jobRequest)
        {
            var driverFolder = PrepareDriverFolder(jobRequest);
            var submissionJobArgsFilePath = CreateBootstrapAvroJobConfig(jobRequest.JobParameters, driverFolder);
            var submissionAppArgsFilePath = CreateBootstrapAvroAppConfig(jobRequest.AppParameters, driverFolder);

            _javaClientLauncher.LaunchAsync(jobRequest.JavaLogLevel, JavaClassName, submissionJobArgsFilePath, submissionAppArgsFilePath)
                .LogAndIgnoreExceptionIfAny(Logger, "Java launcher failed");

            var fileName = Path.Combine(driverFolder, _fileNames.DriverHttpEndpoint);
            JobSubmissionResult result = new LocalJobSubmissionResult(this, fileName);

            var msg = string.Format(CultureInfo.CurrentCulture,
                "Submitted the Driver for execution. Returned driverUrl is: {0}.", result.DriverUrl);
            Logger.Log(Level.Info, msg);
            return result;
        }

        /// <summary>
        /// Return current Job status
        /// </summary>
        /// <returns></returns>
        /// TODO: REEF-889
        [Unstable("0.14", "Working in progress for rest API status returned")]
        public async Task<FinalState> GetJobFinalStatus(string appId)
        {
            await Task.Delay(0);
            return FinalState.SUCCEEDED;
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