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

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    /// <summary>
    /// Base interface defining both the possible failure states and the current status.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public interface IFailureState
    {
        /// <summary>
        /// The current failure state. It is assumed that bigger values mean worst
        /// failure state.
        /// </summary>
        int FailureState { get; set; }

        /// <summary>
        /// A utility method to merge the current failure states and a new one passed as
        /// parameter. The merging is based on user defined semantic.
        /// </summary>
        /// <param name="that">A new failure state</param>
        /// <returns>The merge of the two failure states</returns>
        IFailureState Merge(IFailureState that);
    }
}