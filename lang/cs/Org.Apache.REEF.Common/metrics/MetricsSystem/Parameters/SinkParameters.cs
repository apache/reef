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

using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Common.Metrics.MetricsSystem.Parameters
{
    /// <summary>
    /// Parameters for Sink handlers.
    /// </summary>
    internal sealed class SinkParameters
    {
        [NamedParameter("Maximum snapshots from sources that can be kept in queue of each sink", defaultValue: "1000")]
        public sealed class QueueCapacity : Name<int>
        {
        }

        [NamedParameter("Number of times to retry pushing metrics to sink/consumer if it throws exception",
            defaultValue: "5")]
        public sealed class RetryCount : Name<int>
        {
        }

        [NamedParameter("Min. time interval in milli-seconds between retries for exp. back-off strategy.",
            defaultValue: "1000")]
        public sealed class MinRetryIntervalInMs : Name<int>
        {
        }

        [NamedParameter("Max. time interval in milli-seconds between retries for exp. back-off strategy.",
          defaultValue: "10000")]
        public sealed class MaxRetryIntervalInMs : Name<int>
        {
        }

        [NamedParameter("Degree of randomness in milli-seconds to determine sleep interval for exp. back-off strategy.",
            defaultValue: "1000")]
        public sealed class DeltaBackOffInMs : Name<int>
        {
        }
    }
}
