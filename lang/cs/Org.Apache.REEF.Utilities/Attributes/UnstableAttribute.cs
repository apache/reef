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

namespace Org.Apache.REEF.Utilities.Attributes
{
    /// <summary>
    /// Signals that the API is NOT stabilized.
    /// </summary>
    [AttributeUsage(AttributeTargets.All)]
    public sealed class UnstableAttribute : Attribute
    {
        private readonly string _descriptionOfLikelyChange;
        private readonly string _versionIntroduced;

        /// <summary>
        /// </summary>
        /// <param name="versionIntroduced">The version in which this unstable API was introduced.</param>
        /// <param name="descriptionOfLikelyChange">Description of the likely change in the future.</param>
        public UnstableAttribute(string versionIntroduced, string descriptionOfLikelyChange = "")
        {
            _versionIntroduced = versionIntroduced;
            _descriptionOfLikelyChange = descriptionOfLikelyChange;
        }

        /// <summary>
        /// The version in which this unstable API was introduced.
        /// </summary>
        public string VersionIntroduced
        {
            get { return _versionIntroduced; }
        }

        /// <summary>
        /// Description of the likely change in the future.
        /// </summary>
        public string DescriptionOfLikelyChange
        {
            get { return _descriptionOfLikelyChange; }
        }
    }
}