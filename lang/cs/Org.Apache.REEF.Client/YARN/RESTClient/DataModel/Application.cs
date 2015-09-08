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

namespace Org.Apache.REEF.Client.YARN.RestClient.DataModel
{
    /// <summary>
    /// Class generated based on schema provided in
    /// <see cref="!:http://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/WebServicesIntro.html">
    /// Hadoop RM REST API </see> documentation.
    /// </summary>
    internal sealed class Application
    {
        internal static readonly string Resource = @"cluster/apps/";
        internal static readonly string RootElement = @"app";

        public string Id { get; set; }

        public string User { get; set; }

        public string Name { get; set; }

        public string ApplicationType { get; set; }

        public string Queue { get; set; }

        public State State { get; set; }

        public string FinalStatus { get; set; }

        public float Progress { get; set; }

        public string TrackingUI { get; set; }

        public string TrackingUrl { get; set; }

        public string Diagnostics { get; set; }

        public long ClusterId { get; set; }

        public long StartedTime { get; set; }

        public long FinishedTime { get; set; }

        public long ElapsedTime { get; set; }

        public string AmContainerLogs { get; set; }

        public string AmHostHttpAddress { get; set; }

        public int AllocatedMB { get; set; }

        public int AllocatedVCores { get; set; }

        public int RunningContainers { get; set; }

        public long MemorySeconds { get; set; }

        public long VcoreSeconds { get; set; }
    }
}