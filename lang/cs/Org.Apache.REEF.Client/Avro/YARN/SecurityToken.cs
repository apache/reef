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

namespace Org.Apache.REEF.Client.Avro.YARN
{
    using System.Runtime.Serialization;

    /// <summary>
    /// Used to serialize and deserialize Avro record org.apache.reef.bridge.client.SecurityToken.
    /// </summary>
    [DataContract(Namespace = "org.apache.reef.bridge.client")]
    public partial class SecurityToken
    {
        private const string JsonSchema = @"{""type"":""record"",""name"":""org.apache.reef.bridge.client.SecurityToken"",""doc"":""Security token"",""fields"":[{""name"":""kind"",""doc"":""Kind of the security token"",""type"":""string""},{""name"":""service"",""doc"":""Token service name"",""type"":""string""},{""name"":""key"",""doc"":""Token key"",""type"":""bytes""},{""name"":""password"",""doc"":""Token password"",""type"":""bytes""}]}";

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
        /// Gets or sets the kind field.
        /// </summary>
        [DataMember]
        public string kind { get; set; }
              
        /// <summary>
        /// Gets or sets the service field.
        /// </summary>
        [DataMember]
        public string service { get; set; }
              
        /// <summary>
        /// Gets or sets the key field.
        /// </summary>
        [DataMember]
        public byte[] key { get; set; }
              
        /// <summary>
        /// Gets or sets the password field.
        /// </summary>
        [DataMember]
        public byte[] password { get; set; }
                
        /// <summary>
        /// Initializes a new instance of the <see cref="SecurityToken"/> class.
        /// </summary>
        public SecurityToken()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SecurityToken"/> class.
        /// </summary>
        /// <param name="kind">The kind.</param>
        /// <param name="service">The service.</param>
        /// <param name="key">The key.</param>
        /// <param name="password">The password.</param>
        public SecurityToken(string kind, string service, byte[] key, byte[] password)
        {
            this.kind = kind;
            this.service = service;
            this.key = key;
            this.password = password;
        }
    }
}
