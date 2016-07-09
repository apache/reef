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
    /// Metrics tag class. Tags can be used to group records together at source, sink or other 
    /// appropriate places. They can also be used in MetricsFilter to filter records at higher level.
    /// </summary>
    [Unstable("0.16", "Contract may change.")]
    public sealed class MetricsTag
    {
        private readonly IMetricsInfo _info;
        private readonly string _value;
        
        /// <summary>
        /// Constructor for tags.
        /// </summary>
        /// <param name="info">Meta data for tags.</param>
        /// <param name="value">Value of the tag.</param>
        internal MetricsTag(IMetricsInfo info, string value)
        {
            _info = info;
            _value = value;
        }

        /// <summary>
        /// Name of the tag.
        /// </summary>
        public string Name
        {
            get { return _info.Name; }
        }

        /// <summary>
        /// Description of the tag.
        /// </summary>
        public string Description
        {
            get { return _info.Description; }
        }

        /// <summary>
        /// Info object of the tag.
        /// </summary>
        public IMetricsInfo Info
        {
            get { return _info; }
        }

        /// <summary>
        /// Value of the tag.
        /// </summary>
        public string Value
        {
            get { return _value; }
        }

        /// <summary>
        /// String representation of a tag for display.
        /// </summary>
        /// <returns>The string representation of the tag.</returns>
        public override string ToString()
        {
            return string.Format("Tag Information: {0}, Tag Value: {1}", _info, _value);
        }

        /// <summary>
        /// Checks whether two tags are equal. Relies on Equals 
        /// function of <see cref="IMetricsInfo"/> implementations.
        /// </summary>
        /// <param name="obj">Object to compare against.</param>
        /// <returns>True if both represent the same tag.</returns>
        public override bool Equals(object obj)
        {
            var metricsTag = obj as MetricsTag;
            if (metricsTag != null)
            {
                if (metricsTag.Info.Equals(_info) && metricsTag.Value.Equals(_value))
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Return hash code of the Tag object. Simply uses the hash of ToString() method.
        /// </summary>
        /// <returns>Hash code.</returns>
        public override int GetHashCode()
        {
            var hashCode = ToString().GetHashCode();
            return hashCode;
        }
    }
}
