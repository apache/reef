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

using System.Net;
using System.Net.Sockets;

namespace Org.Apache.REEF.Common.Io
{
    /// <summary>
    /// Tuple containing the string identifier and IPEndpoint.
    /// Used by NameServer and NameClient
    /// </summary>
    public sealed class NameAssignment
    {
        public NameAssignment(string id, IPEndPoint endpoint)
        {
            Identifier = id;
            Endpoint = endpoint;
        }

        public NameAssignment(string id, string address, int port)
        {
            Identifier = id;
            IPAddress ipAddress;
            if (!IPAddress.TryParse(address, out ipAddress))
            {
                IPHostEntry hostEntry = Dns.GetHostEntry(address);
                foreach (var ip in hostEntry.AddressList)
                {
                    if (ip.AddressFamily == AddressFamily.InterNetwork)
                    {
                        ipAddress = ip;
                        break;
                    }
                }
            }
            Endpoint = new IPEndPoint(ipAddress, port);
        }

        public string Identifier { get; set; }

        public IPEndPoint Endpoint { get; set; }
    }
}
