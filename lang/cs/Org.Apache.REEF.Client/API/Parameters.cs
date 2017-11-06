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

namespace Org.Apache.REEF.Client.API.Parameters
{
    /// <summary>
    /// Interval ins ms between connection attempts to the Driver's HTTP Server to obtain Status information.
    /// </summary>
    [NamedParameter(DefaultValue = "500", Documentation = 
        "Interval ins ms between connection attempts to the Driver's HTTP Server to obtain Status information.")]
    internal sealed class DriverHTTPConnectionRetryInterval : Name<int>
    {
        // Intentionally empty
    }

    /// <summary>
    /// Number of Retries when connecting to the Driver's HTTP server.
    /// </summary>
    [NamedParameter(DefaultValue = "10",
        Documentation = "Number of Retries when connecting to the Driver's HTTP server.")]
    internal sealed class DriverHTTPConnectionAttempts : Name<int>
    {
        // Intentionally empty
    }
}
