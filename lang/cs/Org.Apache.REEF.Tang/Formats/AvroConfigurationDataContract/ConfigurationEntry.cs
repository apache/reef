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

namespace Org.Apache.REEF.Tang.Formats.AvroConfigurationDataContract
{
    [DataContract(Name = "ConfigurationEntry", Namespace = "org.apache.reef.tang.formats.avro")]
    [KnownType(typeof(string))]
    public class ConfigurationEntry
    {
        public ConfigurationEntry(string key, string value)
        {
            this.key = key;
            this.value = value;
        }

        public ConfigurationEntry()
        {
        }

        [DataMember]
        public string key { get; set; }

        [DataMember]
        public string value { get; set; }

        public override bool Equals(object obj)
        {
            var that = obj as ConfigurationEntry;
            return that != null && this.Equals(that);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((this.key != null ? this.key.GetHashCode() : 0) * 397) ^ (this.value != null ? this.value.GetHashCode() : 0);
            }
        }

        protected bool Equals(ConfigurationEntry that)
        {
            return this.key == that.key && this.value == that.value;
        }
    }
}
