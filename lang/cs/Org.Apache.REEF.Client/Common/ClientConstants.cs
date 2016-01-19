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

namespace Org.Apache.REEF.Client.Common
{
    /// <summary>
    /// Constants used by the O.A.R.Client project.
    /// </summary>
    internal static class ClientConstants
    {
        /// <summary>
        /// The prefix of the JAR file containing the client logic.
        /// </summary>
        internal const string ClientJarFilePrefix = "reef-bridge-client-";

        /// <summary>
        /// The prefix of the JAR file to be copied to the driver.
        /// </summary>
        internal const string DriverJarFilePrefix = "reef-bridge-java-";
    }
}