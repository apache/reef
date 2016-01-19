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

namespace Org.Apache.REEF.Client.Avro.Local
{
    /// <summary>
    /// Used to serialize and deserialize Avro record org.apache.reef.reef.bridge.client.avro.AvroLocalJobSubmissionParameters.
    /// This is a (mostly) auto-generated class. For instructions on how to regenerate, please view the README.md in the same folder.
    /// </summary>
    [Private]
    [DataContract(Namespace = "org.apache.reef.reef.bridge.client.avro")]
    public sealed class AvroLocalJobSubmissionParameters
    {
        private const string JsonSchema = @"{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.avro.AvroLocalJobSubmissionParameters"",""doc"":""Cross-language submission parameters to the Local runtime"",""fields"":[{""name"":""sharedJobSubmissionParameters"",""type"":{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.avro.AvroJobSubmissionParameters"",""doc"":""General cross-language submission parameters shared by all runtimes"",""fields"":[{""name"":""jobId"",""type"":""string""},{""name"":""tcpBeginPort"",""type"":""int""},{""name"":""tcpRangeCount"",""type"":""int""},{""name"":""tcpTryCount"",""type"":""int""},{""name"":""jobSubmissionFolder"",""type"":""string""}]}},{""name"":""maxNumberOfConcurrentEvaluators"",""type"":""int""}]}";

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
        /// Gets or sets the sharedJobSubmissionParameters field.
        /// </summary>
        [DataMember]
        public AvroJobSubmissionParameters sharedJobSubmissionParameters { get; set; }

        /// <summary>
        /// Gets or sets the maxNumberOfConcurrentEvaluators field.
        /// </summary>
        [DataMember]
        public int maxNumberOfConcurrentEvaluators { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroLocalJobSubmissionParameters"/> class.
        /// </summary>
        public AvroLocalJobSubmissionParameters()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroLocalJobSubmissionParameters"/> class.
        /// </summary>
        /// <param name="sharedJobSubmissionParameters">The sharedJobSubmissionParameters.</param>
        /// <param name="maxNumberOfConcurrentEvaluators">The maxNumberOfConcurrentEvaluators.</param>
        public AvroLocalJobSubmissionParameters(AvroJobSubmissionParameters sharedJobSubmissionParameters, int maxNumberOfConcurrentEvaluators)
        {
            this.sharedJobSubmissionParameters = sharedJobSubmissionParameters;
            this.maxNumberOfConcurrentEvaluators = maxNumberOfConcurrentEvaluators;
        }
    }
}
