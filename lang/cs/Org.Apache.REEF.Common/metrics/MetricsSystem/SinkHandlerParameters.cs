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
using Org.Apache.REEF.Common.Metrics.MetricsSystem.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Metrics.MetricsSystem
{
    /// <summary>
    /// Contains parameters for  <see cref="SinkHandler"/> class.
    /// </summary>
    internal sealed class SinkHandlerParameters
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(SinkHandlerParameters));

        /// <summary>
        /// Constructor. Called by <see cref="MetricsSystem"/> at the time of 
        /// registering the metrics consumer or sink.
        /// </summary>
        /// <param name="queueCapacity">Capacity of the queue or buffer maintained 
        /// for each sink.</param>
        /// <param name="retryCount">In case sink or consumer throws error while consuming metrics, 
        /// how many re-tries to do in exponential back-off setting  before throwing exception.</param>
        /// <param name="minRetryInterval">Minimum wait parameter in milli seconds for exponentail back-off.</param>
        /// <param name="maxRetryInterval">Maximum wait parameter in milli seconds for exponentail back-off.</param>
        /// <param name="deltaBackOff">Degree of randomness in milli seconds in specifying interval before retrying.</param>
        [Inject]
        private SinkHandlerParameters(
            [Parameter(typeof(SinkParameters.QueueCapacity))] int queueCapacity,
            [Parameter(typeof(SinkParameters.RetryCount))] int retryCount,
            [Parameter(typeof(SinkParameters.MinRetryIntervalInMs))] int minRetryInterval,
            [Parameter(typeof(SinkParameters.MaxRetryIntervalInMs))] int maxRetryInterval,
            [Parameter(typeof(SinkParameters.DeltaBackOffInMs))] int deltaBackOff)
        {
            QueueCapacity = queueCapacity;

            PolicyForRetry =
                new RetryPolicy<AllErrorsTransientStrategy>(new ExponentialBackoff(retryCount,
                    TimeSpan.FromMilliseconds(minRetryInterval),
                    TimeSpan.FromMilliseconds(maxRetryInterval),
                    TimeSpan.FromMilliseconds(deltaBackOff)));

            PolicyForRetry.Retrying += (sender, args) =>
            {
                var msg = string.Format("Retry - Count:{0}, Delay:{1}, Exception:{2}",
                    args.CurrentRetryCount,
                    args.Delay,
                    args.LastException);
                Logger.Log(Level.Info, msg);
            };
        }

        /// <summary>
        /// Returns the capacity of thre queue.
        /// </summary>
        public int QueueCapacity { get; private set; }

        /// <summary>
        /// Returns the exponentail back-off policy object for the sink.
        /// </summary>
        public RetryPolicy PolicyForRetry { get; private set; }
    }

    /// <summary>
    /// Specifies that any kind of exception is transient and hence retry should be done
    /// irrespective of the exception type.
    /// </summary>
    internal class AllErrorsTransientStrategy : ITransientErrorDetectionStrategy
    {
        public bool IsTransient(Exception ex)
        {
            return true;
        }
    }
}
