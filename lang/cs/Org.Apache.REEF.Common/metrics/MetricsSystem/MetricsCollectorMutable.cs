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

using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Org.Apache.REEF.Common.Metrics.Api;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Common.Metrics.MetricsSystem
{
    /// <summary>
    /// Default implementation of the <see cref="IMetricsCollectorMutable"/> class. 
    /// It is used to create and maintain collection of records from sources in the 
    /// Metrics System.
    /// </summary>
    internal sealed class MetricsCollectorMutable : IMetricsCollectorMutable
    {
        [Inject]
        private MetricsCollectorMutable()
        {
        }

        private readonly IList<IMetricsRecordBuilder> _recordBuilder = new List<IMetricsRecordBuilder>();

        /// <summary>
        /// Creates a new Record builder by name.
        /// </summary>
        /// <param name="name">Name of the record.</param>
        /// <returns>Newly created Record builder.</returns>
        public IMetricsRecordBuilder CreateRecord(string name)
        {
            var value = new MetricsRecordBuilder(this, new MetricsInfoImpl(name, name));
            _recordBuilder.Add(value);
            return value;
        }

        /// <summary>
        /// Creates a metric record by meta-data info. Creates the new record even 
        /// if another one with same meta-data already exists.
        /// </summary>
        /// <param name="info">Meta-data info of the record.</param>
        /// <returns>Record builder for the record.</returns>
        public IMetricsRecordBuilder CreateRecord(IMetricsInfo info)
        {
            var value = new MetricsRecordBuilder(this, info);
            _recordBuilder.Add(value);
            return value;
        }

        /// <summary>
        /// Creates record from underlying collection of <see cref="IMetricsRecordBuilder"/>
        /// </summary>
        /// <returns>Enumerator over records.</returns>
        public IEnumerable<IMetricsRecord> GetRecords()
        {
            return
                _recordBuilder.Where(builder => !builder.IsEmpty())
                    .Select(builder => builder.GetRecord()).ToArray();
        }

        /// <summary>
        /// Returns an enumerator of <see cref="IMetricsRecordBuilder"/>
        /// </summary>
        /// <returns>Enumerator of <see cref="IMetricsRecordBuilder"/></returns>
        public IEnumerator<IMetricsRecordBuilder> GetEnumerator()
        {
            return _recordBuilder.GetEnumerator();
        }

        /// <summary>
        /// Clears up the metrics collector. Removes all record builders and records.
        /// </summary>
        public void Clear()
        {
            _recordBuilder.Clear();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
