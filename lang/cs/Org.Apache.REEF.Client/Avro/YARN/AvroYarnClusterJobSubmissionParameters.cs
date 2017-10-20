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
using System.Runtime.Serialization;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Client.Avro.YARN
{
    /// <summary>
    /// Used to serialize and deserialize Avro record org.apache.reef.reef.bridge.client.avro.AvroYarnClusterJobSubmissionParameters.
    /// This is a (mostly) auto-generated class. For instructions on how to regenerate, please view the README.md in the same folder.
    /// </summary>
    [Private]
    [DataContract(Namespace = "org.apache.reef.reef.bridge.client.avro")]
    public sealed class AvroYarnClusterJobSubmissionParameters
    {
        private const string JsonSchema = @"{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.avro.AvroYarnClusterJobSubmissionParameters"",""doc"":""Cross-language submission parameters to the YARN runtime using Hadoop's submission client"",""fields"":[{""name"":""yarnJobSubmissionParameters"",""type"":{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.avro.AvroYarnJobSubmissionParameters"",""doc"":""General cross-language submission parameters to the YARN runtime"",""fields"":[{""name"":""sharedJobSubmissionParameters"",""type"":{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.avro.AvroJobSubmissionParameters"",""doc"":""General cross-language job submission parameters shared by all runtimes"",""fields"":[{""name"":""jobId"",""type"":""string""},{""name"":""jobSubmissionFolder"",""type"":""string""}]}},{""name"":""dfsJobSubmissionFolder"",""type"":""string""},{""name"":""fileSystemUrl"",""type"":""string""},{""name"":""jobSubmissionDirectoryPrefix"",""type"":""string""}]}},{""name"":""securityTokenKind"",""type"":""string""},{""name"":""securityTokenService"",""type"":""string""},{""name"":""driverMemory"",""type"":""int""},{""name"":""envMap"",""type"":{""type"":""map"",""values"":""string""}},{""name"":""maxApplicationSubmissions"",""type"":""int""},{""name"":""driverStdoutFilePath"",""type"":""string""},{""name"":""driverStderrFilePath"",""type"":""string""}]}";

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
        /// Gets or sets the yarnJobSubmissionParameters field.
        /// </summary>
        [DataMember]
        public AvroYarnJobSubmissionParameters yarnJobSubmissionParameters { get; set; }

        /// <summary>
        /// Gets or sets the securityTokenKind field.
        /// </summary>
        [DataMember]
        public string securityTokenKind { get; set; }

        /// <summary>
        /// Gets or sets the securityTokenService field.
        /// </summary>
        [DataMember]
        public string securityTokenService { get; set; }

        /// <summary>
        /// Gets or sets the driverMemory field.
        /// </summary>
        [DataMember]
        public int driverMemory { get; set; }

        /// <summary>
        /// Gets or sets the envMap field.
        /// </summary>
        [DataMember]
        public IDictionary<string, string> envMap { get; set; }

        /// <summary>
        /// Gets or sets the maxApplicationSubmissions field.
        /// </summary>
        [DataMember]
        public int maxApplicationSubmissions { get; set; }

        /// <summary>
        /// Gets or sets the driverStdoutFilePath field.
        /// </summary>
        [DataMember]
        public string driverStdoutFilePath { get; set; }

        /// <summary>
        /// Gets or sets the driverStderrFilePath field.
        /// </summary>
        [DataMember]
        public string driverStderrFilePath { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroYarnClusterJobSubmissionParameters"/> class.
        /// </summary>
        public AvroYarnClusterJobSubmissionParameters()
        {
            this.securityTokenKind = "NULL";
            this.securityTokenService = "NULL";
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroYarnClusterJobSubmissionParameters"/> class.
        /// </summary>
        /// <param name="yarnJobSubmissionParameters">The yarnJobSubmissionParameters.</param>
        /// <param name="securityTokenKind">The securityTokenKind.</param>
        /// <param name="securityTokenService">The securityTokenService.</param>
        /// <param name="driverMemory">The driverMemory.</param>
        /// <param name="envMap">The envMap.</param>
        /// <param name="maxApplicationSubmissions">The maxApplicationSubmissions.</param>
        /// <param name="driverStdoutFilePath">The driverStdoutFilePath.</param>
        /// <param name="driverStderrFilePath">The driverStderrFilePath.</param>
        public AvroYarnClusterJobSubmissionParameters(AvroYarnJobSubmissionParameters yarnJobSubmissionParameters, string securityTokenKind, string securityTokenService, int driverMemory, IDictionary<string, string> envMap, int maxApplicationSubmissions, string driverStdoutFilePath, string driverStderrFilePath)
        {
            this.yarnJobSubmissionParameters = yarnJobSubmissionParameters;
            this.securityTokenKind = securityTokenKind;
            this.securityTokenService = securityTokenService;
            this.driverMemory = driverMemory;
            this.envMap = envMap;
            this.maxApplicationSubmissions = maxApplicationSubmissions;
            this.driverStdoutFilePath = driverStdoutFilePath;
            this.driverStderrFilePath = driverStderrFilePath;
        }
    }
}
