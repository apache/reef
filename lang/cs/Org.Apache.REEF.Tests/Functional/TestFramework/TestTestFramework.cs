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
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Client.API.Testing;

namespace Org.Apache.REEF.Tests.Functional.TestFramework
{
    /// <summary>
    /// Tests of REEF's test framework
    /// </summary>
    public sealed class TestTestFramework
    {
        /// <summary>
        /// Tests whether a Driver with a single failing assert is reported correctly.
        /// </summary>
        [Fact]
        public void TestTestFailure()
        {
            ITestRunner testRunner = TestRunnerFactory.NewTestRunner();

            // The TestRunner cannot be null.
            Xunit.Assert.NotNull(testRunner);

            // Submit the job.
            ITestResult testResult = testRunner.RunTest(testRunner.NewJobRequestBuilder()
                .AddDriverConfiguration(TestFailingStartHandler.GetDriverConfiguration())
                .AddGlobalAssemblyForType(typeof(TestFailingStartHandler))
                .SetJobIdentifier("TestFailingTest"));

            // The TestResult cannot be null.
            Xunit.Assert.NotNull(testResult);

            // There should be at least 1 failing assert.
            Xunit.Assert.False(testResult.AllTestsSucceeded, testResult.FailedTestMessage);

            // Only the expected assert should have failed.
            Xunit.Assert.Equal(1, testResult.NumberOfFailedAsserts);
        }

        /// <summary>
        /// Tests whether a Driver with a single passing test is reported correctly.
        /// </summary>
        [Fact]
        public void TestTestPassing()
        {
            ITestRunner testRunner = TestRunnerFactory.NewTestRunner();

            // The TestRunner cannot be null.
            Xunit.Assert.NotNull(testRunner);

            // Submit the job.
            ITestResult testResult = testRunner.RunTest(testRunner.NewJobRequestBuilder()
                .AddDriverConfiguration(TestPassingStartHandler.GetDriverConfiguration())
                .AddGlobalAssemblyForType(typeof(TestPassingStartHandler))
                .SetJobIdentifier("TestPassingTest"));

            // The TestResult cannot be null.
            Xunit.Assert.NotNull(testResult);

            // The TestResult cannot contain a failed assert.
            Xunit.Assert.True(testResult.AllTestsSucceeded, testResult.FailedTestMessage);

            // The TestResult cannot contain more than one passed assert.
            Xunit.Assert.Equal(1, testResult.NumberOfPassedAsserts);
        }
    }

    /// <inheritdoc />
    /// <summary>
    /// A mock test which always fails.
    /// </summary>
    internal sealed class TestFailingStartHandler : IObserver<IDriverStarted>
    {
        private readonly Client.API.Testing.IAssert _assert;

        private const string FailedAssertMessage = "This test should never pass.";

        [Inject]
        private TestFailingStartHandler(Client.API.Testing.IAssert assert, IEvaluatorRequestor evaluatorRequestor)
        {
            _assert = assert;
        }

        public void OnNext(IDriverStarted value)
        {
            // Fail the test case.
            _assert.True(FailedAssertMessage, false);
        }

        public void OnError(Exception error)
        {
            _assert.True("Call to OnError() received.", false);
        }

        public void OnCompleted()
        {
            // empty on purpose.
        }

        public static IConfiguration GetDriverConfiguration()
        {
            return DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<TestFailingStartHandler>.Class)
                .Build();
        }
    }

    /// <summary>
    /// A mock test which always succeeds.
    /// </summary>
    internal sealed class TestPassingStartHandler : IObserver<IDriverStarted>
    {
        private readonly Client.API.Testing.IAssert _assert;

        [Inject]
        private TestPassingStartHandler(Client.API.Testing.IAssert assert)
        {
            _assert = assert;
        }

        public void OnNext(IDriverStarted value)
        {
            _assert.True("This test should always pass.", true);
        }

        public void OnError(Exception error)
        {
            _assert.True("Call to OnError() received.", false);
        }

        public void OnCompleted()
        {
            // empty on purpose.
        }

        public static IConfiguration GetDriverConfiguration()
        {
            return DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<TestPassingStartHandler>.Class)
                .Build();
        }
    }
}