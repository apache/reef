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
using Microsoft.Practices.TransientFaultHandling;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Parameters;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// Handles the retry logic to connect to remote endpoint
    /// </summary>
    internal sealed class RemoteConnectionRetryHandler : IConnectionRetryHandler
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(RemoteConnectionRetryHandler));
        private readonly RetryPolicy<AllErrorsTransientStrategy> _retryPolicy;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="retryCount">Number of times to retry connection</param>
        /// <param name="timeIntervalInMs">Time interval between retries in milli seconds</param>
        [Inject]
        internal RemoteConnectionRetryHandler([Parameter(typeof(ConnectionRetryCount))] int retryCount,
            [Parameter(typeof(SleepTimeInMs))] int timeIntervalInMs)
        {
            var timeIntervalInMs1 = TimeSpan.FromMilliseconds(timeIntervalInMs);
            _retryPolicy = new RetryPolicy<AllErrorsTransientStrategy>(
                    new FixedInterval("ConnectionRetries", retryCount, timeIntervalInMs1, true));
            _retryPolicy.Retrying += (sender, args) =>
            {
                // Log details of the retry.
                var msg = string.Format("Retry - Count:{0}, Delay:{1}, Exception:{2}",
                    args.CurrentRetryCount,
                    args.Delay,
                    args.LastException);
                Logger.Log(Level.Info, msg);
            };
        }

        /// <summary>
        /// Retry policy for the connection
        /// </summary>
        public RetryPolicy Policy
        {
            get
            {
                return _retryPolicy;
            }
        }
    }

    internal class AllErrorsTransientStrategy : ITransientErrorDetectionStrategy
    {
        public bool IsTransient(Exception ex)
        {
            return true;
        }
    }
}
