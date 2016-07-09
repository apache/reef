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
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Metrics.MutableMetricsLayer
{
    /// <summary>
    /// A container interface visible to the user to create and access metric of particular type.
    /// </summary>
    /// <typeparam name="T">Type of mutable metric.</typeparam>
    [Unstable("0.16", "Contract may change.")]
    public interface IMetricContainer<out T> where T : IMutableMetric
    {
        /// <summary>
        /// Creates metric of type T from name and description.
        /// <exception cref="MetricsException">thrown if metric with the name already exists.</exception>
        /// </summary>
        /// <param name="name">Name of the metric.</param>
        /// <param name="desc">Description of the metric.</param>
        /// <returns>Newly created Metric.</returns>
        T Create(string name, string desc);

        /// <summary>
        /// Indexer to access metric by name. 
        /// <exception cref="MetricsException">thrown if metric with that name does not exist.</exception>
        /// </summary>
        /// <param name="name">Name of the metric.</param>
        /// <returns>Metric with given name.</returns>
        T this[string name] { get; }

        /// <summary>
        /// Deletes a metric with given name and description. 
        /// </summary>
        /// <param name="name">Name of the metric.</param>
        /// <returns>True if the metric is deleted, false if it does not exist</returns>
        bool Delete(string name);
    }
}
