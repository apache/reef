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
using System.Collections.Generic;

namespace Org.Apache.REEF.Driver
{
    /// <summary>
    /// Event fired on Driver restarts instead of IDriverStarted.
    /// </summary>
    public interface IDriverRestarted : IDriverStarted
    {
        /// <summary>
        /// The set of expected Evaluator IDs that are returned to the Driver by the
        /// RM on Driver Restart.
        /// </summary>
        ISet<string> ExpectedEvaluatorIds { get; }

        /// <summary>
        /// The number of times the Driver has been resubmitted. Does not include the initial submission.
        /// i.e. ResubmissionAttempts is 0 on first launch and should always be at least 1 when called on
        /// the DriverRestartedHandler.
        /// </summary>
        int ResubmissionAttempts { get; }
    }
}