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
using System.Linq;
using System.Net;
using System.Net.Sockets;

namespace Org.Apache.REEF.Wake.Util
{
    public static class NetworkUtils
    {
        private static IPAddress _localAddress;
        private static readonly Random _random = new Random();

        /// <summary>
        /// Returns the first usable IP Address for the machine.
        /// </summary>
        /// <returns>The machine's local IP Address</returns>
        public static IPAddress LocalIPAddress
        {
            get
            {
                if (_localAddress == null)
                {
                    IPAddress[] localIps = Dns.GetHostAddresses(Dns.GetHostName());
                    _localAddress = localIps.Where(i => i.AddressFamily.Equals(AddressFamily.InterNetwork))
                                            .OrderBy(ip => ip.ToString())
                                            .First();
                }
                
                return _localAddress;
            } 
        }

        /// <summary>
        /// Generate a random port between low (inclusive) and high (exclusive)
        /// </summary>
        /// <param name="low">The inclusive lower bound of the of the port range</param>
        /// <param name="high">The exclusive upper bound of the port range</param>
        /// <returns>The randomly generated port</returns>
        public static int GenerateRandomPort(int low, int high)
        {
            return _random.Next(low, high);
        }
    }
}
