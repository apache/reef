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

using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using RestSharp.Serializers;

namespace Org.Apache.REEF.Client.YARN.RestClient
{
    /// <summary>
    /// RestSharp by default uses SimpleJsonSerializer which 
    /// does not understand property renaming. Here we create a
    /// simple ISerializer implementation that uses Newtonsoft.Json
    /// for performing serialization
    /// </summary>
    internal sealed class RestJsonSerializer : ISerializer
    {
        public RestJsonSerializer()
        {
            ContentType = "application/json";
        }

        public string RootElement { get; set; }

        public string Namespace { get; set; }

        public string DateFormat { get; set; }

        public string ContentType { get; set; }

        public string Serialize(object obj)
        {
            return JsonConvert.SerializeObject(obj, new StringEnumConverter());
        }
    }
}