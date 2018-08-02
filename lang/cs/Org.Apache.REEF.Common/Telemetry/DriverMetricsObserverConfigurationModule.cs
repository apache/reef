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
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Common.Telemetry
{
    /// <summary>
    /// It provides ConfigurationModule for DriverMetrics observers
    /// </summary>
    public sealed class DriverMetricsObserverConfigurationModule : ConfigurationModuleBuilder
    {
        /// <summary>
        /// Observer of driver metrics
        /// </summary>
        public static readonly OptionalImpl<IObserver<IDriverMetrics>> OnDriverMetrics =
            new OptionalImpl<IObserver<IDriverMetrics>>();

        /// <summary>
        /// Configuration module for driver metrics observer.
        /// MetricsService is added as an observer.
        /// User can set more observers with this configuration module.
        /// </summary>
        public readonly static ConfigurationModule ConfigurationModule = new DriverMetricsObserverConfigurationModule()
            .BindSetEntry(GenericType<DriverMetricsObservers>.Class, OnDriverMetrics)
            .BindSetEntry<DriverMetricsObservers, MetricsService, IObserver<IDriverMetrics>>(
                GenericType<DriverMetricsObservers>.Class, GenericType<MetricsService>.Class)
            .Build();
    }
}