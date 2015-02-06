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

//---------- Auto-generated ------------
namespace org.apache.reef.io.network.naming.avro
{
    /// <summary>
    /// Used to serialize and deserialize Avro record org.apache.reef.io.network.naming.avro.AvroNamingRegisterRequest.
    /// </summary>
    [DataContract]
    public class AvroNamingRegisterRequest
    {
        private const string JsonSchema = @"{""type"":""record"",""name"":""org.apache.reef.io.network.naming.avro.AvroNamingRegisterRequest"",""fields"":[{""name"":""id"",""type"":""string""},{""name"":""host"",""type"":""string""},{""name"":""port"",""type"":""int""}]}";

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
        /// Gets or sets the id field.
        /// </summary>
        [DataMember]
        public string id { get; set; }
              
        /// <summary>
        /// Gets or sets the host field.
        /// </summary>
        [DataMember]
        public string host { get; set; }
              
        /// <summary>
        /// Gets or sets the port field.
        /// </summary>
        [DataMember]
        public int port { get; set; }
    }
}
