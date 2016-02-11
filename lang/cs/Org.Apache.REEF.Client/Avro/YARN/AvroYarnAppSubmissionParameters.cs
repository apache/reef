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
    /// Used to serialize and deserialize Avro record org.apache.reef.reef.bridge.client.avro.AvroYarnAppSubmissionParameters.
    /// This is a (mostly) auto-generated class. For instructions on how to regenerate, please view the README.md in the same folder.
    /// </summary>
    [Private]
    [DataContract(Namespace = "org.apache.reef.reef.bridge.client.avro")]
    public sealed class AvroYarnAppSubmissionParameters
    {
        private const string JsonSchema = @"{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.avro.AvroYarnAppSubmissionParameters"",""doc"":""General cross-language application submission parameters to the YARN runtime"",""fields"":[{""name"":""sharedAppSubmissionParameters"",""type"":{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.avro.AvroAppSubmissionParameters"",""doc"":""General cross-language application submission parameters shared by all runtimes"",""fields"":[{""name"":""tcpBeginPort"",""type"":""int""},{""name"":""tcpRangeCount"",""type"":""int""},{""name"":""tcpTryCount"",""type"":""int""}]}},{""name"":""driverRecoveryTimeout"",""type"":""int""}]}";

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
        /// Gets or sets the sharedAppSubmissionParameters field.
        /// </summary>
        [DataMember]
        public AvroAppSubmissionParameters sharedAppSubmissionParameters { get; set; }

        /// <summary>
        /// Gets or sets the driverRecoveryTimeout field.
        /// </summary>
        [DataMember]
        public int driverRecoveryTimeout { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroYarnAppSubmissionParameters"/> class.
        /// </summary>
        public AvroYarnAppSubmissionParameters()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroYarnAppSubmissionParameters"/> class.
        /// </summary>
        /// <param name="sharedAppSubmissionParameters">The sharedAppSubmissionParameters.</param>
        /// <param name="driverRecoveryTimeout">The driverRecoveryTimeout.</param>
        public AvroYarnAppSubmissionParameters(AvroAppSubmissionParameters sharedAppSubmissionParameters, int driverRecoveryTimeout)
        {
            this.sharedAppSubmissionParameters = sharedAppSubmissionParameters;
            this.driverRecoveryTimeout = driverRecoveryTimeout;
        }
    }
}
