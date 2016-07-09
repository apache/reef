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

namespace Org.Apache.REEF.Common.Metrics.MutableMetricsLayer
{
    /// <summary>
    /// Helper class for <see cref="MutableStat"/> to keep and update 
    /// various stats.
    /// </summary>
    internal sealed class StatsHelperClass
    {
        private double _mean = 0;
        private double _unNormalizedVariance = 0;
        private long _numSamples = 0;
        private readonly MinMax _minMax = new MinMax();

        /// <summary>
        /// Copies current stat values fron another instance.
        /// </summary>
        /// <param name="other">Instance from which to copy.</param>
        internal void CopyFrom(StatsHelperClass other)
        {
            _numSamples = other.NumSamples;
            _mean = other.Mean;
            _unNormalizedVariance = other.Variance * (_numSamples - 1);
            _minMax.Reset(other.MinMaxModule);
        }

        /// <summary>
        /// Mean of current samples.
        /// </summary>
        internal double Mean
        {
            get { return _mean; }
        }

        /// <summary>
        /// Total number of samples.
        /// </summary>
        internal long NumSamples
        {
            get { return _numSamples; }
        }

        /// <summary>
        /// Variance of the samples.
        /// </summary>
        internal double Variance
        {
            get { return _numSamples > 1 ? _unNormalizedVariance / (_numSamples - 1) : 0; }
        }

        /// <summary>
        /// Standard deviation of the samples.
        /// </summary>
        internal double Std
        {
            get { return Math.Sqrt(Variance); }
        }

        /// <summary>
        /// Instance of <see cref="MinMax"/> to get min and max values.
        /// </summary>
        internal MinMax MinMaxModule
        {
            get { return _minMax; }
        }

        /// <summary>
        /// Resets the instance.
        /// </summary>
        internal void Reset()
        {
            _unNormalizedVariance = _mean = _numSamples = 0;
            _minMax.Reset();
        }

        /// <summary>
        /// Updates all the stats with the new value. 
        /// Uses Welford method for numerical stability
        /// </summary>
        /// <param name="value">Value to add.</param>
        /// <returns>Returns self.</returns>
        internal StatsHelperClass Add(double value)
        {
            _minMax.Add(value);
            _numSamples++;

            if (_numSamples == 1)
            {
                _mean = value;
            }
            else
            {
                double oldMean = _mean;
                _mean += (value - _mean) / _numSamples;
                _unNormalizedVariance += (value - oldMean) * (value - _mean);
            }
            return this;
        }

        /// <summary>
        /// Helper class for keeping min max vlaue.
        /// </summary>
        internal sealed class MinMax
        {
            const double DefaultMinValue = double.MaxValue;
            const double DefaultMaxValue = double.MinValue;

            private double _min = DefaultMinValue;
            private double _max = DefaultMaxValue;

            /// <summary>
            /// Updates min max values with the new value.
            /// </summary>
            /// <param name="value">Value to add.</param>
            public void Add(double value)
            {
                _min = Math.Min(_min, value);
                _max = Math.Max(_max, value);
            }

            /// <summary>
            /// Resets min and max values to defaults.
            /// </summary>
            public void Reset()
            {
                _min = DefaultMinValue;
                _max = DefaultMaxValue;
            }

            /// <summary>
            /// Resets min max values from some other instance.
            /// </summary>
            /// <param name="other">Instance from which to reset.</param>
            public void Reset(MinMax other)
            {
                _min = other.Min;
                _max = other.Max;
            }

            /// <summary>
            /// Minimum value.
            /// </summary>
            public double Min
            {
                get { return _min; }
            }

            /// <summary>
            /// Maximum value.
            /// </summary>
            public double Max
            {
                get { return _max; }
            }
        }
    }
}