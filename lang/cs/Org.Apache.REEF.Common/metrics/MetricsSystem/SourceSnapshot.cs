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

using System.Collections.Generic;
using Org.Apache.REEF.Common.Metrics.Api;

namespace Org.Apache.REEF.Common.Metrics.MetricsSystem
{
    /// <summary>
    /// Stores snapshot of metric records from a source. 
    /// <see cref="MetricsSourceHandler"/> returns SourceSnapshot 
    /// object when asked for metrics which are then passed over to 
    /// <see cref="SinkHandler"/> for consumption.
    /// </summary>
    internal sealed class SourceSnapshot
    {
        /// <summary>
        /// NAme of the source.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Collection of records from the source to be consumed.
        /// </summary>
        public IEnumerable<IMetricsRecord> Records { get; private set; }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="name">Name of the source.</param>
        /// <param name="records">Collection of records.</param>
        public SourceSnapshot(string name, IEnumerable<IMetricsRecord> records)
        {
            Name = name;
            Records = records;
        }
    }
}
