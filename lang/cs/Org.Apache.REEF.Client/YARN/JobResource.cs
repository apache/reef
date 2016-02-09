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

using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Client.Yarn
{
    [Unstable("New API.")]
    public sealed class JobResource
    {
        public string Name { get; set; }

        public string RemoteUploadPath { get; set; }

        public long LastModificationUnixTimestamp { get; set; }

        public long ResourceSize { get; set; }

        public ResourceType ResourceType { get; set; }
    }
}