/**
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

using System.Runtime.Serialization;

namespace Org.Apache.REEF.Client.Avro.YARN
{
    /// <summary>
    /// Used to serialize and deserialize Avro record org.apache.reef.reef.bridge.client.AvroYarnClusterBootstrapArgs.
    /// </summary>
    [DataContract(Namespace = "org.apache.reef.reef.bridge.client")]
    public sealed class AvroYarnClusterBootstrapArgs
    {
        private const string JsonSchema = @"{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.AvroYarnClusterBootstrapArgs"",""fields"":[{""name"":""yarnBootstrapArgs"",""type"":{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.AvroYarnBootstrapArgs"",""fields"":[{""name"":""sharedBootstrapArgs"",""type"":{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.AvroBootstrapArgs"",""fields"":[{""name"":""jobId"",""type"":""string""},{""name"":""driverMemory"",""type"":""int""},{""name"":""tcpBeginPort"",""type"":""int""},{""name"":""tcpRangeCount"",""type"":""int""},{""name"":""tcpTryCount"",""type"":""int""}]}},{""name"":""driverRecoveryTimeout"",""type"":""int""},{""name"":""jobSubmissionFolder"",""type"":""string""},{""name"":""jobSubmissionDirectoryPrefix"",""type"":""string""}]}},{""name"":""maxApplicationSubmissions"",""type"":""int""},{""name"":""securityTokenKind"",""type"":""string""},{""name"":""securityTokenService"",""type"":""string""}]}";

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
        /// Gets or sets the yarnBootstrapArgs field.
        /// </summary>
        [DataMember]
        public AvroYarnBootstrapArgs yarnBootstrapArgs { get; set; }
              
        /// <summary>
        /// Gets or sets the maxApplicationSubmissions field.
        /// </summary>
        [DataMember]
        public int maxApplicationSubmissions { get; set; }
              
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
        /// Initializes a new instance of the <see cref="AvroYarnClusterBootstrapArgs"/> class.
        /// </summary>
        public AvroYarnClusterBootstrapArgs()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroYarnClusterBootstrapArgs"/> class.
        /// </summary>
        /// <param name="yarnBootstrapArgs">The yarnBootstrapArgs.</param>
        /// <param name="maxApplicationSubmissions">The maxApplicationSubmissions.</param>
        /// <param name="securityTokenKind">The securityTokenKind.</param>
        /// <param name="securityTokenService">The securityTokenService.</param>
        public AvroYarnClusterBootstrapArgs(AvroYarnBootstrapArgs yarnBootstrapArgs, int maxApplicationSubmissions, string securityTokenKind, string securityTokenService)
        {
            this.yarnBootstrapArgs = yarnBootstrapArgs;
            this.maxApplicationSubmissions = maxApplicationSubmissions;
            this.securityTokenKind = securityTokenKind;
            this.securityTokenService = securityTokenService;
        }
    }
}
