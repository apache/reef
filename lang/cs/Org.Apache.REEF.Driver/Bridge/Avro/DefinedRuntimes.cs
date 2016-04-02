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

namespace Org.Apache.REEF.Driver.Bridge.Avro
{
    /// <summary>
    /// Used to serialize and deserialize Avro record org.apache.reef.javabridge.avro.DefinedRuntimes.
    /// </summary>
    [DataContract(Name = "DefinedRuntimes", Namespace = "org.apache.reef.javabridge.avro")]
    [KnownType(typeof(HashSet<string>))]
    public class DefinedRuntimes
    {
        private const string JsonSchema = @"{""type"":""record"",""name"":""org.apache.reef.javabridge.avro.DefinedRuntimes"",""doc"":""Defines the schema for the defined runtime names. This avro object is used to pass runtime names to the c#"",""fields"":[{""name"":""runtimeNames"",""doc"":""defined runtime names"",""type"":{""type"":""array"",""items"":""string""}}]}";

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
        /// Gets or sets the runtimeNames field.
        /// </summary>
        [DataMember]
        public List<string> runtimeNames { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="DefinedRuntimes"/> class.
        /// </summary>
        public DefinedRuntimes()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DefinedRuntimes"/> class.
        /// </summary>
        /// <param name="runtimeNames">The runtimeNames.</param>
        public DefinedRuntimes(ISet<string> runtimeNames)
        {
            this.runtimeNames = new List<string>(runtimeNames);
        }
    }
}
