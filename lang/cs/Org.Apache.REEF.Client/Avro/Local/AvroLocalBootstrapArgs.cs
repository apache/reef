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

namespace Org.Apache.REEF.Client.Avro.Local
{
    /// <summary>
    /// Used to serialize and deserialize Avro record org.apache.reef.reef.bridge.client.AvroLocalBootstrapArgs.
    /// </summary>
    [DataContract(Namespace = "org.apache.reef.reef.bridge.client")]
    public sealed class AvroLocalBootstrapArgs
    {
        private const string JsonSchema = @"{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.AvroLocalBootstrapArgs"",""fields"":[{""name"":""sharedBootstrapArgs"",""type"":{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.AvroBootstrapArgs"",""fields"":[{""name"":""jobId"",""type"":""string""},{""name"":""driverMemory"",""type"":""int""},{""name"":""tcpBeginPort"",""type"":""int""},{""name"":""tcpRangeCount"",""type"":""int""},{""name"":""tcpTryCount"",""type"":""int""}]}},{""name"":""numberOfEvaluators"",""type"":""int""}]}";

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
        /// Gets or sets the numberOfEvaluators field.
        /// </summary>
        [DataMember]
        public int numberOfEvaluators { get; set; }
                
        /// <summary>
        /// Initializes a new instance of the <see cref="AvroLocalBootstrapArgs"/> class.
        /// </summary>
        public AvroLocalBootstrapArgs()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroLocalBootstrapArgs"/> class.
        /// </summary>
        /// <param name="sharedBootstrapArgs">The sharedBootstrapArgs.</param>
        /// <param name="numberOfEvaluators">The numberOfEvaluators.</param>
        public AvroLocalBootstrapArgs(AvroBootstrapArgs sharedBootstrapArgs, int numberOfEvaluators)
        {
            this.sharedBootstrapArgs = sharedBootstrapArgs;
            this.numberOfEvaluators = numberOfEvaluators;
        }
    }
}
