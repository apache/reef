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
    /// Indicates that the attribute target is a breaking change
    /// </summary>
    [AttributeUsage(AttributeTargets.All)]
    public sealed class BreakingAttribute : Attribute
    {
        /// <summary>
        /// </summary>
        /// <param name="versionIntroduced">The version in which this breaking change was introduced.</param>
        public BreakingAttribute(string versionIntroduced)
        {
            VersionIntroduced = versionIntroduced;
        }

        /// <summary>
        /// The version in which this breaking change was introduced.
        /// </summary>
        public string VersionIntroduced { get; private set; }
    }
}
