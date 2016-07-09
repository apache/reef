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

using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Metrics.Api
{
    /// <summary>
    /// Default implementation of the <see cref="IMetricsInfo"/>
    /// </summary>
    [Unstable("0.16", "Contract may change.")]
    public sealed class MetricsInfoImpl : IMetricsInfo
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="name">Name of the metric.</param>
        /// <param name="desc">Description of the metric.</param>
        public MetricsInfoImpl(string name, string desc)
        {
            Name = name;
            Description = desc;
        }

        /// <summary>
        /// Name of the metric.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Description of the metric.
        /// </summary>
        public string Description { get; private set; }

        /// <summary>
        /// Overrides base ToString method.
        /// </summary>
        /// <returns>string representation of the class.</returns>
        public override string ToString()
        {
            return string.Format("Name: {0}, Description: {1}", Name, Description);
        }
    }
}
