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

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    /// <summary>
    /// Interface wrapping an event rised by a transition to a new failure
    /// state. The event speicifies which action have to be executed in response
    /// to the change in the failure state.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public interface IFailureEvent
    {
        /// <summary>
        /// The event / action rised by the transition to a new failure state.
        /// It is assumed that the result encodes the magnituted of the action, 
        /// e.g., smaller number, less demanding action.
        /// </summary>
        /// <returns>A value identifing the magnitued of the event</returns>
        int FailureEvent { get; }
    }
}