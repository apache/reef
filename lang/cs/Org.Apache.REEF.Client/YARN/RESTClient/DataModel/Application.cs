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
    internal sealed class Application
    {
        internal static readonly string Resource = @"cluster/apps/";
        internal static readonly string RootElement = @"app";

        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("user")]
        public string User { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("applicationType")]
        public string ApplicationType { get; set; }

        [JsonProperty("queue")]
        public string Queue { get; set; }

        [JsonProperty("state")]
        public State State { get; set; }

        [JsonProperty("finalStatus")]
        public FinalState FinalStatus { get; set; }

        [JsonProperty("progress")]
        public float Progress { get; set; }

        [JsonProperty("trackingUI")]
        public string TrackingUI { get; set; }

        [JsonProperty("trackingUrl")]
        public string TrackingUrl { get; set; }

        [JsonProperty("diagnostics")]
        public string Diagnostics { get; set; }

        [JsonProperty("clusterId")]
        public long ClusterId { get; set; }

        [JsonProperty("startedTime")]
        public long StartedTime { get; set; }

        [JsonProperty("finishedTime")]
        public long FinishedTime { get; set; }

        [JsonProperty("elapsedTime")]
        public long ElapsedTime { get; set; }

        [JsonProperty("amContainerLogs")]
        public string AmContainerLogs { get; set; }

        [JsonProperty("amHostHttpAddress")]
        public string AmHostHttpAddress { get; set; }

        [JsonProperty("allocatedMB")]
        public int AllocatedMB { get; set; }

        [JsonProperty("allocatedVCores")]
        public int AllocatedVCores { get; set; }

        [JsonProperty("runningContainers")]
        public int RunningContainers { get; set; }

        [JsonProperty("memorySeconds")]
        public long MemorySeconds { get; set; }

        [JsonProperty("vcoreSeconds")]
        public long VcoreSeconds { get; set; }
    }
}