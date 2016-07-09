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

using Org.Apache.REEF.Common.Metrics.Api;

namespace Org.Apache.REEF.Common.Metrics.MutableMetricsLayer
{
    /// <summary>
    /// A convenient metric for throughput measurement. The ValueName field in 
    /// <see cref="MutableStat"/> is set to "Time".
    /// </summary>
    internal sealed class MutableRate : MutableStat, IRate
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="info">Meta-data of the metric.</param>
        /// <param name="extended">If false, outputs only current mean, otherwise outputs 
        /// everything(mean, variance, min, max overall and of current interval.</param>
        public MutableRate(IMetricsInfo info, bool extended = true)
            : base(info, "Time", extended)
        {
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="name">Name of the metric.</param>
        /// <param name="extended">If false, outputs only current mean, otherwise outputs 
        /// everything(mean, variance, min, max overall and of current interval.</param>
        public MutableRate(string name, bool extended = true)
            : base(new MetricsInfoImpl(name, name), "Time", extended)
        {
        }
    }
}
