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

using System.Globalization;
using System.Net;
using System.Text;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// Remote identifier based on a socket address
    /// </summary>
    public sealed class SocketRemoteIdentifier : IRemoteIdentifier
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(SocketRemoteIdentifier));
        private readonly IPEndPoint _addr;

        public SocketRemoteIdentifier(IPEndPoint addr)
        {
            _addr = addr;
        }

        public SocketRemoteIdentifier(string str)
        {
            int index = str.IndexOf(":", System.StringComparison.Ordinal);
            if (index <= 0)
            {
                Exceptions.Throw(new RemoteRuntimeException("Invalid name " + str), LOGGER); 
            }
            string host = str.Substring(0, index);
            int port = int.Parse(str.Substring(index + 1), CultureInfo.InvariantCulture);
            _addr = new IPEndPoint(IPAddress.Parse(host), port);
        }

        public IPEndPoint Addr
        {
            get { return _addr;  }
        }

        public override int GetHashCode()
        {
            return _addr.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            return _addr.Equals(((SocketRemoteIdentifier)obj).Addr);
        }

        public override string ToString()
        {
            StringBuilder builder = new StringBuilder();
            builder.Append("socket://");
            builder.Append(_addr);
            return builder.ToString();
        }
    }
}
