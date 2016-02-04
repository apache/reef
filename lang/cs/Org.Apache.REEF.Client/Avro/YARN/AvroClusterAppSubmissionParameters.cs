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

using System.Runtime.Serialization;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Client.Avro.YARN
{
    /// <summary>
    /// Used to serialize and deserialize Avro record org.apache.reef.reef.bridge.client.avro.AvroYarnClusterAppSubmissionParameters.
    /// </summary>
    [Private]
    [DataContract(Namespace = "org.apache.reef.reef.bridge.client.avro")]
    public sealed class AvroYarnClusterAppSubmissionParameters
    {
        private const string JsonSchema = @"{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.avro.AvroYarnClusterAppSubmissionParameters"",""doc"":""Cross-language application submission parameters to the YARN runtime using Hadoop's submission client"",""fields"":[{""name"":""yarnAppSubmissionParameters"",""type"":{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.avro.AvroYarnAppSubmissionParameters"",""doc"":""General cross-language application submission parameters to the YARN runtime"",""fields"":[{""name"":""sharedAppSubmissionParameters"",""type"":{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.avro.AvroAppSubmissionParameters"",""doc"":""General cross-language application submission parameters shared by all runtimes"",""fields"":[{""name"":""tcpBeginPort"",""type"":""int""},{""name"":""tcpRangeCount"",""type"":""int""},{""name"":""tcpTryCount"",""type"":""int""}]}},{""name"":""driverMemory"",""type"":""int""},{""name"":""driverRecoveryTimeout"",""type"":""int""}]}},{""name"":""maxApplicationSubmissions"",""type"":""int""}]}";

        /// <summary>
        /// Gets the schema.
        /// </summary>
        public static string Schema
        {
            get
            {
                return JsonSchema;
            }
        }

        /// <summary>
        /// Gets or sets the yarnAppSubmissionParameters field.
        /// </summary>
        [DataMember]
        public AvroYarnAppSubmissionParameters yarnAppSubmissionParameters { get; set; }

        /// <summary>
        /// Gets or sets the maxApplicationSubmissions field.
        /// </summary>
        [DataMember]
        public int maxApplicationSubmissions { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroYarnClusterAppSubmissionParameters"/> class.
        /// </summary>
        public AvroYarnClusterAppSubmissionParameters()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroYarnClusterAppSubmissionParameters"/> class.
        /// </summary>
        /// <param name="yarnAppSubmissionParameters">The yarnAppSubmissionParameters.</param>
        /// <param name="maxApplicationSubmissions">The maxApplicationSubmissions.</param>
        public AvroYarnClusterAppSubmissionParameters(AvroYarnAppSubmissionParameters yarnAppSubmissionParameters, int maxApplicationSubmissions)
        {
            this.yarnAppSubmissionParameters = yarnAppSubmissionParameters;
            this.maxApplicationSubmissions = maxApplicationSubmissions;
        }
    }
}
