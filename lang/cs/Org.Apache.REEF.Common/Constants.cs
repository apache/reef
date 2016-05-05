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
    public static class Constants
    {
        /// <summary>
        /// The local HTTP Endpoint used for local runtime HTTP Server tests.
        /// </summary>
        public const string LocalHttpEndpointBaseUri = @"http://localhost:8080/";  

        /// <summary>
        /// The base endpoint to redirect HTTP requests to the Driver's HTTP Server on HDInsight.
        /// </summary>
        public const string HDInsightClusterHttpEndpointBaseUri = @"http://headnodehost:9014/proxy/";

        /// <summary>
        /// The v1 REEF HTTP API URI specification of a REEF Job.
        /// </summary>
        public const string HttpReefUriSpecification = @"Reef/v1/";

        /// <summary>
        /// The relative path to Driver on a REEF HTTP Server request.
        /// </summary>
        public const string HttpDriverUriTarget = @"Driver/";

        /// <summary>
        /// The Name Server name.
        /// </summary>
        public const string NameServerServiceName = "NameServer";

        /// <summary>
        /// The Environment variable used to discover the YARN Application ID of the REEF Job.
        /// </summary>
        public const string ReefYarnApplicationIdEnvironmentVariable = "REEF_YARN_APPLICATION_ID";

        /// <summary>
        /// The log that is printed on a successful Evaluator exit.
        /// </summary>
        public const string EvaluatorExitSuccessLog = "The Evaluator has successfully exited.";

        /// <summary>
        /// The log that is printed on an Evaluator failure.
        /// </summary>
        public const string EvaluatorExitFailureLog = "The Evaluator has exited on a failure.";
    }
}
