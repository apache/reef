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
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Text;
using Org.Apache.REEF.Common.Catalog.Capabilities;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Catalog
{
    internal sealed class ResourceCatalogImpl : IResourceCatalog
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ResourceCatalogImpl));
        
        private static readonly string DefaultRackName = "default-rack";

        private readonly Dictionary<string, RackDescriptorImpl> _racks = new Dictionary<string, RackDescriptorImpl>();
 
        private readonly Dictionary<string, NodeDescriptorImpl> _nodes = new Dictionary<string, NodeDescriptorImpl>();

        public string Name { get; set; }

        public ICollection<ICapability> Capabilities { get; set; }

        public ICollection<INodeDescriptor> Nodes { get; set; }

        public ICollection<IRackDescriptor> Racks { get; set; }

        public INodeDescriptor GetNode(string nodeId)
        {
            return _nodes[nodeId];
        }

        public void Handle(NodeDescriptorProto node)
        {
            string rackName = node.rack_name == null ? node.rack_name : DefaultRackName;
            string message = string.Format(
                CultureInfo.InvariantCulture,
                "Catalog new node: id[{0}], rack[{1}], host[{2}], port[{3}], memory[{4}]",
                node.identifier,
                rackName,
                node.host_name,
                node.port,
                node.memory_size);
            LOGGER.Log(Level.Info, message);
            if (!string.IsNullOrWhiteSpace(rackName) && !_racks.ContainsKey(rackName))
            {
                RackDescriptorImpl newRack = new RackDescriptorImpl(rackName);
                _racks.Add(rackName, newRack);
            }
            RackDescriptorImpl rack = _racks[rackName];
            IPAddress ipAddress = null;
            IPAddress.TryParse(node.host_name, out ipAddress);
            if (ipAddress == null)
            {
                throw new ArgumentException("cannot parse host ipaddress: " + node.host_name);
            }
            IPEndPoint ipEndPoint = new IPEndPoint(ipAddress, node.port);
            RAM ram = new RAM(node.memory_size);
            NodeDescriptorImpl nodeDescriptor = new NodeDescriptorImpl(node.identifier, ipEndPoint, rack, ram);
            _nodes.Add(nodeDescriptor.Id, nodeDescriptor);
        }

        public override string ToString()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("=== Resource Catalog ===");
            foreach (IRackDescriptor rackDescriptor in Racks)
            {
                stringBuilder.Append(Environment.NewLine + rackDescriptor);
            }
            return stringBuilder.ToString();
        }
    }
}
