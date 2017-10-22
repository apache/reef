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

using Org.Apache.REEF.Client.Local.TestRunner;
using Org.Apache.REEF.Utilities.Attributes;
using System;

namespace Org.Apache.REEF.Client.API.Testing
{
    /// <summary>
    /// Factory for TestRunner instances.
    /// </summary>
    /// <remarks>
    /// This class follows the same approach as org.apache.reef.tests.TestEnvironmentFactory in Java. It reads the 
    /// same environment variables to decide which test runner to instantiate.
    /// </remarks>
    [Unstable("0.17", "Work in progress towards a new test infrastructure. See REEF-1271.")]
    public sealed class TestRunnerFactory
    {
        // See `org.apache.reef.tests.TestEnvironmentFactory` in Java.
        private const string TEST_ON_YARN_ENVIRONMENT_VARIABLE = "REEF_TEST_YARN";

        /// <summary>
        /// Instantiates a TestRunner based on the environment variables.
        /// </summary>
        /// <returns>A TestRunner instance.</returns>
        public static ITestRunner NewTestRunner()
        {
            if (RunOnYarn())
            {
                throw new NotImplementedException("Running tests on YARN is not supported yet.");
            }
            else
            {
                return LocalTestRunner.GetLocalTestRunner();
            }
        }

        /// <summary>
        /// Check whether the tests are supposed to be run on YARN.
        /// </summary>
        /// <returns>True, if the tests are supposed to run on YARN.</returns>
        private static bool RunOnYarn()
        {
            return !string.IsNullOrEmpty(Environment.GetEnvironmentVariable(TEST_ON_YARN_ENVIRONMENT_VARIABLE));
        }      
    }
}
