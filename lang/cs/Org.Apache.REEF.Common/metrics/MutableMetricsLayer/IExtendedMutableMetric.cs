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

namespace Org.Apache.REEF.Common.Metrics.MutableMetricsLayer
{
    /// <summary>
    /// Extends the <see cref="IMutableMetric"/> with methods to be used internally. These
    /// functions will not be visible to the users.
    /// </summary>
    internal interface IExtendedMutableMetric : IMutableMetric
    {
        /// <summary>
        /// Sets the changed flag. Called in derived classes when metric values are changed.
        /// </summary>
        void SetChanged();

        /// <summary>
        /// Clears the changed flag. Called by snapshot operations after recording latest values.
        /// </summary>
        void ClearChanged();

        /// <summary>
        /// True if metric value changed after taking a snapshot, false otherwise.
        /// </summary>
        bool Changed { get; }
    }
}
