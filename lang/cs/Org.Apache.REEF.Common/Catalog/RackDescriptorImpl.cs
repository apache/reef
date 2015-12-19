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
using System.Collections.Generic;
using System.Text;
using Org.Apache.REEF.Common.Catalog.Capabilities;

namespace Org.Apache.REEF.Common.Catalog
{
    internal sealed class RackDescriptorImpl : IRackDescriptor
    {
        public RackDescriptorImpl(string name)
        {
            Name = name;
            Capabilities = new List<ICapability>();
            Nodes = new List<INodeDescriptor>();
        }

        public string Name { get; set; }

        public ICollection<ICapability> Capabilities { get; set; }

        public ICollection<INodeDescriptor> Nodes { get; set; }

        public ICollection<IRackDescriptor> Racks { get; set; }

        public INodeDescriptor GetNode(string nodeId)
        {
            throw new NotImplementedException();
        }

        public void AddNodeDescriptor(NodeDescriptorImpl node)
        {
            Nodes.Add(node);
        }

        public override string ToString()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("Rack " + Name);
            foreach (INodeDescriptor nodeDescriptor in Nodes)
            {
                stringBuilder.Append(Environment.NewLine + nodeDescriptor);
            }
            return stringBuilder.ToString();
        }

        public override int GetHashCode()
        {
            return Name.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }
    }
}
