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
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.Avro.YARN
{
    /// <summary>
    /// Used to serialize and deserialize Avro record org.apache.reef.reef.bridge.client.AvroYarnBootstrapArgs.
    /// </summary>
    [DataContract(Namespace = "org.apache.reef.reef.bridge.client")]
    public sealed class AvroYarnBootstrapArgs
    {
        private const string JsonSchema = @"{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.AvroYarnBootstrapArgs"",""fields"":[{""name"":""sharedBootstrapArgs"",""type"":{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.AvroBootstrapArgs"",""fields"":[{""name"":""jobId"",""type"":""string""},{""name"":""tcpBeginPort"",""type"":""int""},{""name"":""tcpRangeCount"",""type"":""int""},{""name"":""tcpTryCount"",""type"":""int""},{""name"":""jobSubmissionFolder"",""type"":""string""}]}},{""name"":""driverMemory"",""type"":""int""},{""name"":""driverRecoveryTimeout"",""type"":""int""},{""name"":""jobSubmissionDirectoryPrefix"",""type"":""string""}]}";

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
        /// Gets or sets the sharedBootstrapArgs field.
        /// </summary>
        [DataMember]
        public AvroBootstrapArgs sharedBootstrapArgs { get; set; }

        /// <summary>
        /// Gets or sets the driverMemory field.
        /// </summary>
        [DataMember]
        public int driverMemory { get; set; }

        /// <summary>
        /// Gets or sets the driverRecoveryTimeout field.
        /// </summary>
        [DataMember]
        public int driverRecoveryTimeout { get; set; }
              
        /// <summary>
        /// Gets or sets the jobSubmissionDirectoryPrefix field.
        /// </summary>
        [DataMember]
        public string jobSubmissionDirectoryPrefix { get; set; }
                
        /// <summary>
        /// Initializes a new instance of the <see cref="AvroYarnBootstrapArgs"/> class.
        /// </summary>
        public AvroYarnBootstrapArgs()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroYarnBootstrapArgs"/> class.
        /// </summary>
        /// <param name="sharedBootstrapArgs">The sharedBootstrapArgs.</param>
        /// <param name="driverMemory">The driverMemory.</param>
        /// <param name="driverRecoveryTimeout">The driverRecoveryTimeout.</param>
        /// <param name="jobSubmissionDirectoryPrefix">The jobSubmissionDirectoryPrefix.</param>
        public AvroYarnBootstrapArgs(AvroBootstrapArgs sharedBootstrapArgs, int driverMemory, int driverRecoveryTimeout, string jobSubmissionDirectoryPrefix)
        {
            this.sharedBootstrapArgs = sharedBootstrapArgs;
            this.driverMemory = driverMemory;
            this.driverRecoveryTimeout = driverRecoveryTimeout;
            this.jobSubmissionDirectoryPrefix = jobSubmissionDirectoryPrefix;
        }
    }
}
