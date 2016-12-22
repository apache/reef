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
using Org.Apache.REEF.Common.Metrics.MetricsSystem.Parameters;
using Org.Apache.REEF.Common.Metrics.MutableMetricsLayer;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Metrics.MetricsSystem
{
    /// <summary>
    /// This configuration module defines configuration for <see cref="MetricsSystem"/>.
    /// </summary>
    [Unstable("0.16", "Contract may change.")]
    public sealed class MetricsSystemConfiguration : ConfigurationModuleBuilder
    {
        /// <summary>
        /// Optional source filter to filter out certain sources.
        /// </summary>
        public static readonly OptionalImpl<IMetricsFilter> SourceFilter = new OptionalImpl<IMetricsFilter>();

        /// <summary>
        /// Periodic time after which snapshot will be taken.
        /// </summary>
        public static readonly OptionalParameter<int> PeriodicTimer = new OptionalParameter<int>();

        /// <summary>
        /// Whether to get even unchanged metrics from the sources.
        /// </summary>
        public static readonly OptionalParameter<bool> GetUnchangedMetrics = new OptionalParameter<bool>();

        /// <summary>
        /// Capacity of the queue at each sink where snapshot from sources are kept.
        /// </summary>
        public static readonly OptionalParameter<int> SinkQueueCapacity = new OptionalParameter<int>();

        /// <summary>
        /// Number of times we try pushing metrics to the sink before throwing exception.
        /// </summary>
        public static readonly OptionalParameter<int> SinkRetries = new OptionalParameter<int>();

        /// <summary>
        /// Minimum retry interval for exponetial back-off strategy for retry.
        /// </summary>
        public static readonly OptionalParameter<int> SinkMinRetryIntervalInMs = new OptionalParameter<int>();

        /// <summary>
        /// Maximum retry interval for exponetial back-off strategy for retry.    
        /// /// </summary>
        public static readonly OptionalParameter<int> SinkMaxRetryIntervalInMs = new OptionalParameter<int>();

        /// <summary>
        /// Degree of randomness in milli seconds in specifying interval before retrying
        /// </summary>
        public static readonly OptionalParameter<int> DeltaBackOffInMs = new OptionalParameter<int>();

        /// <summary>
        /// This configuration module set see<see cref="IMetricsSource"/> as <see cref="DefaultMetricsSourceImpl"/>
        /// </summary>
        public static ConfigurationModule ConfigurationModule
        {
            get
            {
                return new MetricsSystemConfiguration()
                    .BindImplementation(GenericType<IMetricsSystem>.Class, GenericType<MetricsSystem>.Class)
                    .BindImplementation(GenericType<IMetricsCollectorMutable>.Class,
                        GenericType<MetricsCollectorMutable>.Class)
                    .BindImplementation(GenericType<IMetricsFilter>.Class, SourceFilter)
                    .BindNamedParameter(GenericType<MetricsSystemPeriodicTimer>.Class, PeriodicTimer)
                    .BindNamedParameter(GenericType<MetricsSystemGetUnchangedMetrics>.Class, GetUnchangedMetrics)
                    .BindNamedParameter(GenericType<SinkQueueCapacity>.Class, SinkQueueCapacity)
                    .BindNamedParameter(GenericType<SinkRetryCount>.Class, SinkRetries)
                    .BindNamedParameter(GenericType<SinkMinRetryIntervalInMs>.Class, SinkMinRetryIntervalInMs)
                    .BindNamedParameter(GenericType<SinkMaxRetryIntervalInMs>.Class, SinkMaxRetryIntervalInMs)
                    .BindNamedParameter(GenericType<SinkDeltaBackOffInMs>.Class, DeltaBackOffInMs)
                    .Build();
            }
        }
    }
}