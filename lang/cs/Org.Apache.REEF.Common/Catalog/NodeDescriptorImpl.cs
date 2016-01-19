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

using System.Collections.Generic;
using System.Net;
using Org.Apache.REEF.Common.Catalog.Capabilities;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Catalog
{
    [Private]
    internal sealed class NodeDescriptorImpl : INodeDescriptor
    {
        private readonly RackDescriptorImpl _rack;

        private readonly string _id;

        private IPEndPoint _address;

        private RAM _ram;

        private readonly IList<ICapability> _capabilities;

        public NodeDescriptorImpl()
        {
        }

        public NodeDescriptorImpl(string id, IPEndPoint addresss, RackDescriptorImpl rack, RAM ram)
        {
            _id = id;
            _address = addresss;
            _rack = rack;
            _ram = ram;
            _capabilities = new List<ICapability>();
            _rack.AddNodeDescriptor(this);
        }

        public RackDescriptorImpl Rack 
        {
            get
            {
                return _rack;
            }
        }

        public string Id
        {
            get
            {
                return _id;
            }
        }

        public string HostName { get; set; }

        public CPU Cpu
        {
            get
            {
                return new CPU(1);
            }

            set
            {
            }
        }

        public RAM Ram
        {
            get
            {
                return _ram;
            }

            set
            {
                _ram = value;
            }
        }

        public IList<ICapability> Capabilities
        {
            get
            {
                return _capabilities;
            }
        }

        public IPEndPoint InetSocketAddress
        {
            get
            {
                return _address;
            }

            set
            {
                _address = value;
            }
        }
    }
}
