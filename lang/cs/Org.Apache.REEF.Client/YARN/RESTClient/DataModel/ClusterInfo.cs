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
    /// <see cref="!:http://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/WebServicesIntro.html">
    /// Hadoop RM REST API </see> documentation.
    /// </summary>
    internal sealed class ClusterInfo
    {
        internal static readonly string Resource = @"cluster/info";
        internal static readonly string RootElement = @"clusterInfo";

        [JsonProperty("id")]
        public long Id { get; set; }

        [JsonProperty("startedOn")]
        public long StartedOn { get; set; }

        [JsonProperty("state")]
        public ClusterState State { get; set; }

        [JsonProperty("haState")]
        public ClusterHaState HaState { get; set; }

        [JsonProperty("resourceManagerVersion")]
        public string ResourceManagerVersion { get; set; }

        [JsonProperty("resourceManagerBuildVersion")]
        public string ResourceManagerBuildVersion { get; set; }

        [JsonProperty("resourceManagerVersionBuiltOn")]
        public string ResourceManagerVersionBuiltOn { get; set; }

        [JsonProperty("hadoopVersion")]
        public string HadoopVersion { get; set; }

        [JsonProperty("hadoopBuildVersion")]
        public string HadoopBuildVersion { get; set; }

        [JsonProperty("hadoopVersionBuiltOn")]
        public string HadoopVersionBuiltOn { get; set; }
    }
}