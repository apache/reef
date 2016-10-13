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
using Org.Apache.REEF.Driver.Bridge.Clr2java;

namespace Org.Apache.REEF.Driver.Bridge.Events
{
    /// <summary>
    /// The implementation of IDriverRestarted.
    /// </summary>
    internal sealed class DriverRestarted : IDriverRestarted
    {
        private readonly ISet<string> _expectedEvaluatorIds;

        internal DriverRestarted(IDriverRestartedClr2Java driverRestartedClr2Java)
        {
            _expectedEvaluatorIds = new HashSet<string>(driverRestartedClr2Java.GetExpectedEvaluatorIds());
            StartTime = driverRestartedClr2Java.GetStartTime();
            ResubmissionAttempts = driverRestartedClr2Java.GetResubmissionAttempts();
        }

        public ISet<string> ExpectedEvaluatorIds
        {
            get { return new HashSet<string>(_expectedEvaluatorIds); }
        }

        public DateTime StartTime { get; }

        public int ResubmissionAttempts { get; }
    }
}