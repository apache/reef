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

using System;

namespace Org.Apache.REEF.Common
{
    public class Constants
    {
        // if 8080 port is not used, then query would fail, 
        // this is only for local runtime testing purpose though, so it should be ok
        public const string LocalHttpEndpointBaseUri = @"http://localhost:8080/";  

        public const string HDInsightClusterHttpEndpointBaseUri = @"http://headnodehost:9014/proxy/";

        public const string HttpReefUriSpecification = @"Reef/v1/";

        public const string HttpDriverUriTarget = @"Driver/";

        public const string NameServerServiceName = "NameServer";

        public const string ReefYarnApplicationIdEnvironmentVariable = "REEF_YARN_APPLICATION_ID";
    }
}
