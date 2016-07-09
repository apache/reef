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
    /// Interface for collecting the metrics. This interface is passed to 
    /// the <see cref="IMetricsSource"/> to add and fill in the records.
    /// </summary>
    [Unstable("0.16", "Contract may change.")]
    public interface IMetricsCollector
    {
        /// <summary>
        /// Creates a metric record by name. The exact semantics of what to do if another 
        /// record of same name exists - (create another with same name, return existing one, 
        /// or throw exception) is left to the implementation.
        /// </summary>
        /// <param name="name">Name of the record.</param>
        /// <returns>Record builder for the record.</returns>
        IMetricsRecordBuilder CreateRecord(string name);

        /// <summary>
        /// Creates a metric record by meta-data info. The exact semantics of what to do if another 
        /// record of same name exists - (create another with same name, return existing one, 
        /// or throw exception) is left to the implementation.
        /// </summary>
        /// <param name="info">Meta-data info of the record.</param>
        /// <returns>Record builder for the record.</returns>
        IMetricsRecordBuilder CreateRecord(IMetricsInfo info);
    }
}
