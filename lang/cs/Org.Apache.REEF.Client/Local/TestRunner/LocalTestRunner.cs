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
using System.IO;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.Local.TestRunner.FileWritingAssert;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Client.API.Testing;

namespace Org.Apache.REEF.Client.Local.TestRunner
{
    /// <summary>
    /// Runs a test on the local runtime.
    /// </summary>
    internal sealed class LocalTestRunner : ITestRunner
    {
        private static readonly Logger LOG = Logger.GetLogger(typeof(LocalTestRunner));
        private readonly IREEFClient _client;

        [Inject]
        private LocalTestRunner(IREEFClient client)
        {
            _client = client;
        }

        public JobRequestBuilder NewJobRequestBuilder()
        {
            return _client.NewJobRequestBuilder();
        }

        public ITestResult RunTest(JobRequestBuilder jobRequestBuilder)
        {
            // Setup the assert file.
            var assertFileName = Path.GetTempPath() + "/reef-test-" + DateTime.Now.ToString("yyyyMMddHHmmssfff") + ".json";
            jobRequestBuilder.AddDriverConfiguration(FileWritingAssertConfiguration.ConfigurationModule
                    .Set(FileWritingAssertConfiguration.FilePath, assertFileName)
                    .Build());
            var jobRequest = jobRequestBuilder.Build();

            LOG.Log(Level.Info, "Submitting job `{0}` for execution. Assert log in `{1}`",
                jobRequest.JobIdentifier,
                assertFileName);
            IJobSubmissionResult jobStatus = _client.SubmitAndGetJobStatus(jobRequest);

            if (null == jobStatus)
            {
                return TestResult.Fail(
                    "JobStatus returned by the Client was null. This points to an environment setup problem.");
            }

            LOG.Log(Level.Verbose, "Waiting for job `{0}` to complete.", jobRequest.JobIdentifier);
            jobStatus.WaitForDriverToFinish();
            LOG.Log(Level.Verbose, "Job `{0}` completed.", jobRequest.JobIdentifier);

            return ReadTestResult(assertFileName);
        }

        private static TestResult ReadTestResult(string assertFilePath)
        {
            if (!File.Exists(assertFilePath))
            {
                return TestResult.Fail("Test Results file {0} does not exist.", assertFilePath);
            }

            try
            {
                return TestResult.FromJson(File.ReadAllText(assertFilePath))
                    ?? TestResult.Fail("Results read from `{0}` where null.", assertFilePath);
            }
            catch (Exception exception)
            {
                return TestResult.Fail("Could not parse test results: {0}", exception);
            }
        }

        /// <summary>
        /// Convenience method to generate a local test runner with the given number of containers.
        /// </summary>
        /// <param name="numberOfContainers"></param>
        /// <returns></returns>
        public static ITestRunner GetLocalTestRunner(int numberOfContainers = 4)
        {
            return GetLocalTestRunner(
                LocalRuntimeClientConfiguration.ConfigurationModule
                    .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators, numberOfContainers.ToString())
                    .Build());
        }

        /// <summary>
        /// Convenience method to instantiate a local test runner with the given runtime Configuration.
        /// </summary>
        /// <param name="runtimeConfiguration"></param>
        /// <returns></returns>
        public static ITestRunner GetLocalTestRunner(IConfiguration runtimeConfiguration)
        {
            return TangFactory.GetTang().NewInjector(runtimeConfiguration).GetInstance<LocalTestRunner>();
        }
    }
}
