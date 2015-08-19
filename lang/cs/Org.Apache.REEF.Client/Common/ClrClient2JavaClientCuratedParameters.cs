/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote.Parameters;

namespace Org.Apache.REEF.Client.Common
{
    /// <summary>
    /// Curated parameters for CLR to Java. Passes a set of command line parameters to YarnJobSubmissionClient on
    /// the Java side. The command line parameters should be strictly ordered.
    /// Note that the EnableRestart parameter will only be true if the user ever binds a DriverRestartedHandler.
    /// </summary>
    internal class ClrClient2JavaClientCuratedParameters
    {
        public int TcpPortRangeStart { get; private set; }
        public int TcpPortRangeCount { get; private set; }
        public int TcpPortRangeTryCount { get; private set; }
        public int TcpPortRangeSeed { get; private set; }
        public int MaxApplicationSubmissions { get; private set; }
        public bool EnableRestart { get; private set; }


        [Inject]
        private ClrClient2JavaClientCuratedParameters(
            [Parameter(typeof(TcpPortRangeStart))] int tcpPortRangeStart,
            [Parameter(typeof(TcpPortRangeCount))] int tcpPortRangeCount,
            [Parameter(typeof(TcpPortRangeTryCount))] int tcpPortRangeTryCount,
            [Parameter(typeof(TcpPortRangeSeed))] int tcpPortRangeSeed,
            [Parameter(typeof(DriverBridgeConfigurationOptions.MaxApplicationSubmissions))] int maxApplicationSubmissions,
            [Parameter(typeof(DriverBridgeConfigurationOptions.RestartEnabled))] bool restartEnabled)
        {
            TcpPortRangeStart = tcpPortRangeStart;
            TcpPortRangeCount = tcpPortRangeCount;
            TcpPortRangeTryCount = tcpPortRangeTryCount;
            TcpPortRangeSeed = tcpPortRangeSeed;
            MaxApplicationSubmissions = maxApplicationSubmissions;
            EnableRestart = restartEnabled;
        }
    }
}
