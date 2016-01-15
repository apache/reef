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

using Newtonsoft.Json;

namespace Org.Apache.REEF.Client.YARN.RestClient.DataModel
{
    /// <summary>
    /// Class generated based on schema provided in
    /// <a href="http://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/WebServicesIntro.html">
    /// Hadoop RM REST API</a> documentation.
    /// </summary>
    internal sealed class ClusterMetrics
    {
        internal static readonly string Resource = @"cluster/metrics";
        internal static readonly string RootElement = @"clusterMetrics";

        [JsonProperty("appsSubmitted")]
        public int AppsSubmitted { get; set; }

        [JsonProperty("appsCompleted")]
        public int AppsCompleted { get; set; }

        [JsonProperty("appsPending")]
        public int AppsPending { get; set; }

        [JsonProperty("appsRunning")]
        public int AppsRunning { get; set; }

        [JsonProperty("appsFailed")]
        public int AppsFailed { get; set; }

        [JsonProperty("appsKilled")]
        public int AppsKilled { get; set; }

        [JsonProperty("reservedMB")]
        public long ReservedMB { get; set; }

        [JsonProperty("availableMB")]
        public long AvailableMB { get; set; }

        [JsonProperty("allocatedMB")]
        public long AllocatedMB { get; set; }

        [JsonProperty("totalMB")]
        public long TotalMB { get; set; }

        [JsonProperty("reservedVirtualCores")]
        public long ReservedVirtualCores { get; set; }

        [JsonProperty("availableVirtualCores")]
        public long AvailableVirtualCores { get; set; }

        [JsonProperty("allocatedVirtualCores")]
        public long AllocatedVirtualCores { get; set; }

        [JsonProperty("totalVirtualCores")]
        public long TotalVirtualCores { get; set; }

        [JsonProperty("containersAllocated")]
        public int ContainersAllocated { get; set; }

        [JsonProperty("containersReserved")]
        public int ContainersReserved { get; set; }

        [JsonProperty("containersPending")]
        public int ContainersPending { get; set; }

        [JsonProperty("totalNodes")]
        public int TotalNodes { get; set; }

        [JsonProperty("activeNodes")]
        public int ActiveNodes { get; set; }

        [JsonProperty("lostNodes")]
        public int LostNodes { get; set; }

        [JsonProperty("unhealthyNodes")]
        public int UnhealthyNodes { get; set; }

        [JsonProperty("decommissionedNodes")]
        public int DecommissionedNodes { get; set; }

        [JsonProperty("rebootedNodes")]
        public int RebootedNodes { get; set; }
    }
}