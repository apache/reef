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
    internal sealed class ClusterMetrics
    {
        internal static readonly string Resource = @"cluster/metrics";
        internal static readonly string RootElement = @"clusterMetrics";

        public int AppsSubmitted { get; set; }

        public int AppsCompleted { get; set; }

        public int AppsPending { get; set; }

        public int AppsRunning { get; set; }

        public int AppsFailed { get; set; }

        public int AppsKilled { get; set; }

        public long ReservedMB { get; set; }

        public long AvailableMB { get; set; }

        public long AllocatedMB { get; set; }

        public long TotalMB { get; set; }

        public long ReservedVirtualCores { get; set; }

        public long AvailableVirtualCores { get; set; }

        public long AllocatedVirtualCores { get; set; }

        public long TotalVirtualCores { get; set; }

        public int ContainersAllocated { get; set; }

        public int ContainersReserved { get; set; }

        public int ContainersPending { get; set; }

        public int TotalNodes { get; set; }

        public int ActiveNodes { get; set; }

        public int LostNodes { get; set; }

        public int UnhealthyNodes { get; set; }

        public int DecommissionedNodes { get; set; }

        public int RebootedNodes { get; set; }
    }
}