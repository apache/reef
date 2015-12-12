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
using Org.Apache.REEF.Tang.Types;

namespace Org.Apache.REEF.Tang.Implementations.ClassHierarchy
{
    public class PackageNodeImpl : AbstractNode, IPackageNode
    {
        public PackageNodeImpl(INode parent, string name, string fullName) : 
            base(parent, name, fullName)
        {
        }

        public PackageNodeImpl()
            : base(null, string.Empty, "[root node]")
        {
        }

        /**
        * Unlike normal nodes, the root node needs to take the package name of its
        * children into account.  Therefore, we use the full name as the key when
        * we insert nodes into the root.
        */
        public override void Add(INode n) 
        {
            children.Add(n.GetFullName(), n);
        }
    }
}
