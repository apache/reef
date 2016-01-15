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

using System.Collections.Generic;
using Newtonsoft.Json;

namespace Org.Apache.REEF.Client.YARN.RestClient.DataModel
{
    /// <summary>
    /// Class generated based on schema provided in
    /// <a href="http://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/WebServicesIntro.html">
    /// Hadoop RM REST API</a> documentation.
    /// </summary>
    internal sealed class SubmitApplication
    {
        internal static readonly string Resource = @"cluster/apps";

        [JsonProperty(PropertyName = "application-id")]
        public string ApplicationId { get; set; }

        [JsonProperty(PropertyName = "application-name")]
        public string ApplicationName { get; set; }

        [JsonProperty(PropertyName = "queue")]
        public string Queue { get; set; }

        [JsonProperty(PropertyName = "priority")]
        public int Priority { get; set; }

        [JsonProperty(PropertyName = "am-container-spec")]
        public AmContainerSpec AmContainerSpec { get; set; }

        [JsonProperty(PropertyName = "unmanaged-AM")]
        public bool UnmanagedAM { get; set; }

        [JsonProperty(PropertyName = "max-app-attempts")]
        public int MaxAppAttempts { get; set; }

        [JsonProperty(PropertyName = "resource")]
        public Resouce AmResource { get; set; }

        [JsonProperty(PropertyName = "application-type")]
        public string ApplicationType { get; set; }

        [JsonProperty(PropertyName = "keep-containers-across-application-attempts")]
        public bool KeepContainersAcrossApplicationAttempts { get; set; }

        [JsonProperty(PropertyName = "application-tags")]
        public IList<ApplicationTag> ApplicationTags { get; set; }
    }
}