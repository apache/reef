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

using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Comm.Impl
{
    /// <summary>
    /// Class defining the updates of the topology for a running task.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal sealed class TopologyUpdate : ICloneable
    {
        /// <summary>
        /// Create an update for a node containing both the list of children and the root node.
        /// </summary>
        /// <param name="node">The node receiving the update</param>
        /// <param name="children">The update to the children of the node</param>
        /// <param name="root">The update for the root of the node</param>
        public TopologyUpdate(string node, List<string> children, string root)
        {
            Node = node;
            Children = children;
            Root = root;
        }

        /// <summary>
        /// Create an update for a node containing only the list of children.
        /// </summary>
        /// <param name="node">The node receiving the update</param>
        /// <param name="children">The update to the children of the node</param>
        public TopologyUpdate(string node, List<string> children) : this(node, children, string.Empty)
        {
        }

        /// <summary>
        /// Create an update for a node containing only the root node.
        /// </summary>
        /// <param name="node">The node receiving the update</param>
        /// <param name="root">The update for the root of the node</param>
        public TopologyUpdate(string node, string root) : this(node, new List<string>(), root)
        {
        }

        /// <summary>
        /// The node receiving the update.
        /// </summary>
        public string Node { get; private set; }

        /// <summary>
        /// The updates for the children.
        /// </summary>
        public List<string> Children { get; set; }

        /// <summary>
        /// The updates for the root.
        /// </summary>
        public string Root { get; private set; }

        /// <summary>
        /// The total memory size for the update (used for serialization).
        /// </summary>
        public int Size
        {
            get
            {
                // 1 int for the size of node
                // The size of node
                // 1 int for the number of children
                // 1 int for the length of each children
                // The size of the string of each child
                // 1 int + the size of root if not null
                var nodeSize = sizeof(int) + Node.Length;
                var childrenSize = sizeof(int) + (Children.Count * sizeof(int)) + Children.Sum(x => x.Length);
                var rootSize = sizeof(int) + Root.Length;

                return nodeSize + childrenSize + rootSize;
            }
        }

        /// <summary>
        /// Serialize the update.
        /// </summary>
        /// <param name="buffer">The memory space where to copy the serialized update</param>
        /// <param name="offset">Where to start writing in the buffer</param>
        /// <param name="updates">The updates to serialize</param>
        internal static void Serialize(byte[] buffer, ref int offset, IEnumerable<TopologyUpdate> updates)
        {
            byte[] tmpBuffer;

            foreach (var value in updates)
            {
                Buffer.BlockCopy(BitConverter.GetBytes(value.Node.Length), 0, buffer, offset, sizeof(int));
                offset += sizeof(int);
                tmpBuffer = ByteUtilities.StringToByteArrays(value.Node);
                Buffer.BlockCopy(tmpBuffer, 0, buffer, offset, tmpBuffer.Length);
                offset += tmpBuffer.Length;

                Buffer.BlockCopy(BitConverter.GetBytes(value.Children.Count), 0, buffer, offset, sizeof(int));
                offset += sizeof(int);
                foreach (var child in value.Children)
                {
                    tmpBuffer = ByteUtilities.StringToByteArrays(child);
                    Buffer.BlockCopy(BitConverter.GetBytes(tmpBuffer.Length), 0, buffer, offset, sizeof(int));
                    offset += sizeof(int);
                    Buffer.BlockCopy(tmpBuffer, 0, buffer, offset, tmpBuffer.Length);
                    offset += tmpBuffer.Length;
                }

                if (value.Root == null)
                {
                    Buffer.BlockCopy(BitConverter.GetBytes(0), 0, buffer, offset, sizeof(int));
                    offset += sizeof(int);
                }
                else
                {
                    tmpBuffer = ByteUtilities.StringToByteArrays(value.Root);
                    Buffer.BlockCopy(BitConverter.GetBytes(tmpBuffer.Length), 0, buffer, offset, sizeof(int));
                    offset += sizeof(int);
                    Buffer.BlockCopy(tmpBuffer, 0, buffer, offset, tmpBuffer.Length);
                    offset += tmpBuffer.Length;
                }
            }
        }

        /// <summary>
        /// Deserialize the update.
        /// </summary>
        /// <param name="data">The memory space where to fetch the serialized updates/param>
        /// <param name="totLength">The total memory size of the serialized updates</param>
        /// <param name="start">Where to start reading in the buffer</param>
        internal static List<TopologyUpdate> Deserialize(byte[] data, int totLength, int start)
        {
            var result = new List<TopologyUpdate>();
            var num = 0;
            var length = 0;
            var offset = 0;
            string value;
            string node;
            List<string> tmp;

            while (offset < totLength)
            {
                length = BitConverter.ToInt32(data, start + offset);
                offset += sizeof(int);
                node = ByteUtilities.ByteArraysToString(data, start + offset, length);
                offset += length;

                num = BitConverter.ToInt32(data, start + offset);
                offset += sizeof(int);
                tmp = new List<string>();
                for (int i = 0; i < num; i++)
                {
                    length = BitConverter.ToInt32(data, start + offset);
                    offset += sizeof(int);
                    value = ByteUtilities.ByteArraysToString(data, start + offset, length);
                    offset += length;
                    tmp.Add(value);
                }

                length = BitConverter.ToInt32(data, start + offset);
                offset += sizeof(int);
                if (length > 0)
                {
                    value = ByteUtilities.ByteArraysToString(data, start + offset, length);
                    offset += length;
                    result.Add(new TopologyUpdate(node, tmp, value));
                }
                else
                {
                    result.Add(new TopologyUpdate(node, tmp));
                }
            }

            return result;
        }

        public object Clone()
        {
            return new TopologyUpdate(Node, Children, Root);
        }
    }
}
