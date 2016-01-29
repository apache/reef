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
using Newtonsoft.Json;

namespace Org.Apache.REEF.Tang.Formats.AvroConfigurationDataContract
{
    [KnownType(typeof(HashSet<ConfigurationEntry>))]
    [DataContract(Name = "AvroConfiguration", Namespace = "org.apache.reef.tang.formats.avro")]
    public sealed class AvroConfiguration
    {
        public AvroConfiguration()
        {
        }

        public AvroConfiguration(string language, ISet<ConfigurationEntry> bindings)
        {
            // TODO: [REEF-276] AvroSerializer currently does not serialize HashSets
            // correctly, so using a List for now to get around the issue.
            // An ISet is still passed in to guarantee configuration uniqueness.
            this.Bindings = new List<ConfigurationEntry>(bindings);
            this.language = language;
        }

        [DataMember]
        public string language { get; set; }

        [DataMember]
        public List<ConfigurationEntry> Bindings { get; set; }

        public static AvroConfiguration GetAvroConfigurationFromEmbeddedString(string jsonString)
        {
            return JsonConvert.DeserializeObject<AvroConfiguration>(jsonString);
        }
    }
}
