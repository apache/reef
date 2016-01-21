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

using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Network.Naming.Events;
using Org.Apache.REEF.Wake.RX;

namespace Org.Apache.REEF.Network.Naming.Observers
{
    /// <summary>
    /// Handler for registering an identifier and endpoint with the Name Service
    /// </summary>
    internal sealed class NamingRegisterRequestObserver : AbstractObserver<NamingRegisterRequest>
    {
        private readonly NameServer _server;

        public NamingRegisterRequestObserver(NameServer server)
        {
            _server = server;
        }

        /// <summary>
        /// Register the identifier and IPEndpoint with the NameServer and send 
        /// the response back to the NameClient
        /// </summary>
        /// <param name="value">The register request event</param>
        public override void OnNext(NamingRegisterRequest value)
        {
            NameAssignment assignment = value.NameAssignment;
            _server.Register(assignment.Identifier, assignment.Endpoint);

            value.Link.Write(new NamingRegisterResponse(value));
        }
    }
}
