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
using System.Reactive;
using Org.Apache.REEF.Common.Metrics.Api;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Metrics.MutableMetricsLayer
{
    /// <summary>
    /// Implementation for the stat metric. Samples will be 
    /// added by <see cref="Sample"/> function call.
    /// </summary>
    internal class MutableStat : MutableMetricBase, IStat
    {
        private readonly IMetricsInfo _numSamplesInfo;
        private readonly IMetricsInfo _runningMeanInfo;
        private readonly IMetricsInfo _currentMeanInfo;
        private readonly IMetricsInfo _runningMinInfo;
        private readonly IMetricsInfo _runningMaxInfo;
        private readonly IMetricsInfo _currentMinInfo;
        private readonly IMetricsInfo _currentMaxInfo;
        private readonly IMetricsInfo _runningStdInfo;
        private readonly IMetricsInfo _currentStdInfo;
        private readonly object _lock = new object();

        private readonly StatsHelperClass _runningStat = new StatsHelperClass();
        private readonly StatsHelperClass _intervalStat = new StatsHelperClass();
        private readonly StatsHelperClass _prevStat = new StatsHelperClass();
        private readonly bool _showExtendedStats;
        private static readonly Logger Logger = Logger.GetLogger(typeof(MutableStat));

        /// <summary>
        /// Value this stat represents (Time, Latency etc.). Used to generate description 
        /// for average, variance metrics etc. For example: "Average Time for ...." or 
        /// "Average Latency for ........".
        /// </summary>
        protected readonly string ValueName;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="info">Meta data of the stat.</param>
        /// <param name="valueName">Value (e.g. time, latency) the stat represents.</param>
        /// <param name="extendedStats"></Whether to show only mean (false) or mean, 
        /// stdev, min, max etc.</param>
        public MutableStat(IMetricsInfo info, string valueName, bool extendedStats = true)
            : base(info)
        {
            ValueName = valueName;
            _showExtendedStats = extendedStats;

            string name = info.Name + "-Num";
            string desc = "Number of samples for " + info.Description;
            _numSamplesInfo = new MetricsInfoImpl(name, desc);

            name = info.Name + "-RunningAvg";
            desc = "Average " + valueName + " for " + info.Description;
            _runningMeanInfo = new MetricsInfoImpl(name, desc);

            name = info.Name + "-RunningStdev";
            desc = "Standard deviation of " + valueName + " for " + info.Description;
            _runningStdInfo = new MetricsInfoImpl(name, desc);

            name = info.Name + "-IntervalAvg";
            desc = "Interval Average " + valueName + " for " + info.Description;
            _currentMeanInfo = new MetricsInfoImpl(name, desc);

            name = info.Name + "-IntervalStdev";
            desc = "Interval Standard deviation of " + valueName + " for " + info.Description;
            _currentStdInfo = new MetricsInfoImpl(name, desc);

            name = info.Name + "-RunningMin";
            desc = "Min " + valueName + " for " + info.Description;
            _runningMinInfo = new MetricsInfoImpl(name, desc);

            name = info.Name + "-RunningMax";
            desc = "Max " + valueName + " for " + info.Description;
            _runningMaxInfo = new MetricsInfoImpl(name, desc);

            name = info.Name + "-IntervalMin";
            desc = "Interval Min " + valueName + " for " + info.Description;
            _currentMinInfo = new MetricsInfoImpl(name, desc);

            name = info.Name + "-IntervalMax";
            desc = "Interval Max " + valueName + " for " + info.Description;
            _currentMaxInfo = new MetricsInfoImpl(name, desc);

            RegisterSnapshotRequestObserver(Observer.Create<SnapshotRequest>(this.GiveSnapshot,
               this.SnapshotError,
               UnSubscribe));
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="name">Name of the stat.</param>
        /// <param name="valueName">Value (e.g. time, latency) the stat represents.</param>
        /// <param name="extendedStats"></Whether to show only mean (false) or mean, 
        /// stdev, min, max etc.</param>
        public MutableStat(string name, string valueName, bool extendedStats = true) 
            : this(new MetricsInfoImpl(name, name), valueName, extendedStats)
        {
        }

        /// <summary>
        /// Adds a sample to the stat.
        /// </summary>
        /// <param name="value">Value of the sample.</param>
        public void Sample(double value)
        {
            lock (_lock)
            {
                _runningStat.Add(value);
                _intervalStat.Add(value);
                SetChanged();
            }
        }

        private void GiveSnapshot(SnapshotRequest request)
        {
            bool all = request.FullSnapshot;
            var recordBuilder = request.Builder;
            lock (_lock)
            {
                var lastStat = Changed ? _intervalStat : _prevStat;
                if (all || Changed)
                {
                    recordBuilder.AddCounter(_numSamplesInfo, _runningStat.NumSamples)
                        .AddGauge(_currentMeanInfo, lastStat.Mean);

                    if (_showExtendedStats)
                    {
                        recordBuilder.AddGauge(_currentMaxInfo, _intervalStat.MinMaxModule.Max)
                            .AddGauge(_currentMinInfo, _intervalStat.MinMaxModule.Min)
                            .AddGauge(_currentStdInfo, _intervalStat.Std)
                            .AddGauge(_runningMaxInfo, _runningStat.MinMaxModule.Max)
                            .AddGauge(_runningMinInfo, _runningStat.MinMaxModule.Min)
                            .AddGauge(_runningMeanInfo, _runningStat.Mean)
                            .AddGauge(_runningStdInfo, _runningStat.Std);
                    }

                    if (Changed)
                    {
                        if (_runningStat.NumSamples > 0)
                        {
                            _prevStat.CopyFrom(_intervalStat);
                            _intervalStat.Reset();
                        }
                        ClearChanged();
                    }
                }
            }
        }

        private void SnapshotError(Exception e)
        {
            Logger.Log(Level.Error, "Exception happened while trying to take the snapshot");
        }
    }
}
