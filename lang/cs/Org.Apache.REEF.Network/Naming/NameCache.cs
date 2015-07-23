/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Net;
using System.Runtime.Caching;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Network.Naming
{
    internal class NameCache
    {
        private readonly MemoryCache _cache;
        private readonly double _expirationDuration;

        [Inject]
        private NameCache(
            [Parameter(typeof (NameCacheConfiguration.CacheEntryExpiryTime))] double expirationDuration)
        {
            _cache = new MemoryCache("NameClientCache");
            _expirationDuration = expirationDuration;
        }

        public void Set(string identifier, IPEndPoint value)
        {
            _cache.Set(identifier, value, DateTimeOffset.Now.AddMilliseconds(_expirationDuration));
        }

        public IPEndPoint Get(string identifier)
        {
            var entry = _cache.Get(identifier);
            return entry as IPEndPoint;
        }

        public void RemoveEntry(string identifier)
        {
            _cache.Remove(identifier);
        }
    }
}
