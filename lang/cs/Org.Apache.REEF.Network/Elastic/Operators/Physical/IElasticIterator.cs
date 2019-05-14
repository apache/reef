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
using System;
using System.Collections;

namespace Org.Apache.REEF.Network.Elastic.Operators.Physical
{
    /// <summary>
    /// Group communication operator used to for iterations.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public interface IElasticIterator : IElasticOperator, IEnumerator
    {
        /// <summary>
        /// Synchronize the current iteration with the input one.
        /// </summary>
        /// <param name="iteration">The state in which the iterator will be moved</param>
        void SyncIteration(int iteration);

        /// <summary>
        /// Register the action to trigger when a task is rescheduled.
        /// </summary>
        /// <param name="action">Some code to execute upon task rescheduling</param>
        void RegisterActionOnTaskRescheduled(Action action);
    }
}
