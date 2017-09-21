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

using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.YARN.Parameters
{
    //// TODO: [JIRA REEF-1887] Deprecated. Remove in REEF 0.18..
    [NamedParameter("Security token kind.", defaultValue: DefaultTokenKind)]
    public sealed class SecurityTokenKindParameter : Name<string>
    {
        public const string DefaultTokenKind = "NULL";
    }

    //// TODO: [JIRA REEF-1887] Deprecated. Remove in REEF 0.18..
    [NamedParameter("Security token service name.", defaultValue: DefaultService)]
    public sealed class SecurityTokenServiceParameter : Name<string>
    {
        public const string DefaultService = "NULL";
    }
}