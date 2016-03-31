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
using System.Net;
using System.Net.Sockets;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// Default implementation of ITcpClientFactory.
    /// Provides TcpClient for the remote endpoint with 
    /// the specified retry logic for connection.
    /// </summary>
    public sealed class TcpClientConnectionFactory : ITcpClientConnectionFactory
    {
        private readonly IConnectionRetryHandler _retryHandler;
        private static readonly Logger Logger = Logger.GetLogger(typeof(TcpClientConnectionFactory));

        [Inject]
        private TcpClientConnectionFactory(IConnectionRetryHandler retryHandler)
        {
            _retryHandler = retryHandler;
        }

        /// <summary>
        /// Provides TcpClient for the specific endpoint with the 
        /// retry logic provided by the user.
        /// </summary>
        /// <param name="endPoint">IP address and port number of server</param>
        /// <returns>Tcp client</returns>
        public TcpClient Connect(IPEndPoint endPoint)
        {
            TcpClient client = new TcpClient();

            try
            {
                _retryHandler.Policy.ExecuteAction(() => client.Connect(endPoint));
                var msg = string.Format("Connection to endpoint {0} established", endPoint);
                Logger.Log(Level.Info, msg);
                return client;
            }
            catch (Exception e)
            {
                var msg = string.Format("Connection to endpoint {0} failed", endPoint);
                Exceptions.CaughtAndThrow(e, Level.Error, msg, Logger);
                return null;
            }
        }
    }
}
