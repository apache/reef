﻿// Licensed to the Apache Software Foundation (ASF) under one
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

using StringMetric = Org.Apache.REEF.Common.Telemetry.MetricBase<string>;

namespace Org.Apache.REEF.Common.Telemetry
{
    /// <summary>
    /// A simple driver metrics implementation that contains the system state.
    /// </summary>
    public sealed class DriverMetrics : IDriverMetrics
    {
        private MetricsData _metrics;

        public IMetric SystemState
        {
            get;
        }

        private string _stateMetricName = "DriverState";

        public DriverMetrics(string systemState)
        {
            _metrics = new MetricsData();
            SystemState = new StringMetric(_stateMetricName, "driver state.", DateTime.Now.Ticks, systemState);
            _metrics.TryRegisterMetric(SystemState);
        }

        public MetricsData GetMetricsData()
        {
            return _metrics;
        }
    }
}
