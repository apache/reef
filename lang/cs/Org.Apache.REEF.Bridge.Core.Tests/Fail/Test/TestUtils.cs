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
using Org.Apache.REEF.Common.Client;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Bridge.Core.Tests.Fail.Test
{
    internal static class TestUtils
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(TestUtils));

        public static void AssertLauncherFailure(LauncherStatus status, Type expectedFailureType)
        {
            Assert.Equal(LauncherStatus.FailedStatus, status);
            var ex = status.Error.OrElse(null);
            Assert.True(HasCause(ex, expectedFailureType), "Unexpected error: " + ex);
        }

        private static bool HasCause(Exception ex, Type exceptionType)
        {
            for (var exception = ex; exception != null; exception = exception.InnerException)
            {
                Log.Log(Level.Info, "Exception type {0}", exception.GetType());
                if (exceptionType.IsInstanceOfType(exception))
                {
                    return true;
                }
                Log.Log(Level.Info, "Not instance of {0}", exceptionType);
            }
            return false;
        }
    }
}
