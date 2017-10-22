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
        private IREEFClient Client { get; }

        [Inject]
        private LocalTestRunner(IREEFClient client)
        {
            Client = client;
        }

        public JobRequestBuilder NewJobRequestBuilder()
        {
            return Client.NewJobRequestBuilder();
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
            IJobSubmissionResult jobStatus = Client.SubmitAndGetJobStatus(jobRequest);

            if (null == jobStatus)
            {
                TestResult result = new TestResult();
                result.RecordAssertResult(
                    "JobStatus returned by the Client was null. The points to an environment setup problem.", false);
                return result;
            }

            LOG.Log(Level.Verbose, "Waiting for job `{0}` to complete.", jobRequest.JobIdentifier);
            jobStatus.WaitForDriverToFinish();
            LOG.Log(Level.Verbose, "Job `{0}` completed.", jobRequest.JobIdentifier);

            return ReadTestResult(assertFileName);
        }

        private static TestResult ReadTestResult(string assertFilePath)
        {
            if (File.Exists(assertFilePath))
            {
                try
                {
                    string textFromDisk = File.ReadAllText(assertFilePath);
                    if (string.IsNullOrWhiteSpace(textFromDisk))
                    {
                        TestResult result = new TestResult();
                        result.RecordAssertResult(string.Format("The assert file `{0}` was empty", assertFilePath), false);
                        return result;
                    }
                    else
                    {
                        TestResult result = TestResult.FromJson(textFromDisk);
                        if (result == null)
                        {
                            result = new TestResult();
                            result.RecordAssertResult(string.Format("Results read from `{0}` where null.", assertFilePath), false);
                        }
                        return result;
                    }
                }
                catch (Exception exception)
                {
                    var result = new TestResult();
                    result.RecordAssertResult("Could not parse test results: " + exception, false);
                    return result;
                }
            }
            else
            {
                var result = new TestResult();
                result.RecordAssertResult("Test Results file could not be read.", false);
                return result;
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