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

namespace Org.Apache.REEF.Network.Naming.Parameters
{
    public static class NameCacheConfiguration
    {
        [NamedParameter("Time interval in milliseconds before the cache entry expires", "timeinterval", "300000")]
        public class CacheEntryExpiryTime : Name<double>
        {
        }

        [NamedParameter("Maximum cache memory in MB", "cachememorylimit", "20")]
        public class CacheMemoryLimit : Name<string>
        {
        }

        [NamedParameter("Polling interval for checking cache memory", "cachepollinginterval", "00:20:00")]
        public class PollingInterval : Name<string>
        {
        }
    }
}
