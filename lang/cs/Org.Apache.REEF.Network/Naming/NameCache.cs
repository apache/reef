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
using System.Collections.Specialized;
using System.Net;
using System.Runtime.Caching;
using Org.Apache.REEF.Network.Naming.Parameters;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Network.Naming
{
    /// <summary>
    /// Cache class for caching IpEndPoint Lookups
    /// </summary>
    internal class NameCache
    {
        private readonly MemoryCache _cache;

        /// <summary>
        /// Duration in milli seconds after which cache entry expires
        /// Usage in cache requires it to be double than int or long
        /// </summary>
        private readonly double _expirationDuration;

        [Inject]
        private NameCache(
            [Parameter(typeof(NameCacheConfiguration.CacheEntryExpiryTime))] double expirationDuration,
            [Parameter(typeof(NameCacheConfiguration.CacheMemoryLimit))] string memoryLimit,
            [Parameter(typeof(NameCacheConfiguration.PollingInterval))] string pollingInterval)
        {
            var config = new NameValueCollection
            {
                { "pollingInterval", pollingInterval },
                { "physicalMemoryLimitPercentage", "0" },
                { "cacheMemoryLimitMegabytes", memoryLimit }
            };

            _cache = new MemoryCache("NameClientCache", config);
            _expirationDuration = expirationDuration;
        }

        /// <summary>
        /// Add an entry to cache if it does not exist or replace if it already ecists
        /// </summary>
        /// <param name="identifier">remote destination Id</param>
        /// <param name="value">IPEndPoint of remote destination</param>
        internal void Set(string identifier, IPEndPoint value)
        {
            _cache.Set(identifier, value, DateTimeOffset.Now.AddMilliseconds(_expirationDuration));
        }

        /// <summary>
        /// Gets the cached remote IpEndPoint given the name
        /// </summary>
        /// <param name="identifier">Remote destination name/Id</param>
        /// <returns>IPEndPoint of remote destination if it is cached, null otherwise</returns>
        internal IPEndPoint Get(string identifier)
        {
            var entry = _cache.Get(identifier);
            return entry as IPEndPoint;
        }

        /// <summary>
        /// Removes the entry from the cache
        /// </summary>
        /// <param name="identifier"></param>
        internal void RemoveEntry(string identifier)
        {
            _cache.Remove(identifier);
        }

        /// <summary>
        /// returns physical memory of the cache in MB
        /// </summary>
        internal long PhysicalMemoryLimit
        {
            get { return _cache.CacheMemoryLimit; }
        }

        /// <summary>
        /// returns the interval after which Cache checks its memory usage
        /// </summary>
        internal TimeSpan PollingInterval
        {
            get { return _cache.PollingInterval; }
        }
    }
}
