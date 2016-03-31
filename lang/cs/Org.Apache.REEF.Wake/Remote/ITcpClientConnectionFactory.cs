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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.Wake.Remote
{
    /// <summary>
    /// Provides TcpClient for the remote endpoint
    /// </summary>
    [DefaultImplementation(typeof(TcpClientConnectionFactory))]
    public interface ITcpClientConnectionFactory
    {
        /// <summary>
        /// Provides TcpClient for the specific endpoint
        /// </summary>
        /// <param name="endPoint">IP address and port number of server</param>
        /// <returns>Tcp client</returns>
        TcpClient Connect(IPEndPoint endPoint);
    }
}
