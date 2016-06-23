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

using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    /// <summary>
    /// A container for children nodes in Group Communications.
    /// </summary>
    [NotThreadSafe]
    internal sealed class ChildNodeContainer<T> : IEnumerable<NodeStruct<T>>
    {
        private readonly Dictionary<string, NodeStruct<T>> _childIdToNodeMap = 
            new Dictionary<string, NodeStruct<T>>();

        /// <summary>
        /// Gets the number of children.
        /// </summary>
        public int Count
        {
            get { return _childIdToNodeMap.Count; }
        }

        /// <summary>
        /// Puts the child node into the container.
        /// </summary>
        public void PutNode(NodeStruct<T> childNode)
        {
            _childIdToNodeMap.Add(childNode.Identifier, childNode);
        }

        /// <summary>
        /// Gets the child with the specified identifier.
        /// </summary>
        public bool TryGetChild(string identifier, out NodeStruct<T> child)
        {
            return _childIdToNodeMap.TryGetValue(identifier, out child);
        }

        /// <summary>
        /// Gets the child with the specified identifier. Returns null if child does not exist.
        /// </summary>
        public NodeStruct<T> GetChild(string identifier)
        {
            NodeStruct<T> child;
            TryGetChild(identifier, out child);
            return child;
        }

        /// <summary>
        /// Gets the data from all children nodes synchronously.
        /// </summary>
        public IEnumerable<T> GetDataFromAllChildren()
        {
            return this.SelectMany(child => child.GetData());
        }

        /// <summary>
        /// Gets an Enumerator for iterating through children nodes.
        /// </summary>
        public IEnumerator<NodeStruct<T>> GetEnumerator()
        {
            return _childIdToNodeMap.Values.GetEnumerator();
        }

        /// <summary>
        /// Gets an Enumerator for iterating through children nodes.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}