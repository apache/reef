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
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Implementations.ClassHierarchy
{
    internal abstract class AbstractNode : INode
    {
        /// It is from Type.FullName. This name is used as Name in a Node. 
        /// It is not unique for a generic type with different type of arguments.
        /// It is used for toString or debug info as AssemblyQualifiedName is really long
        private readonly string name;

        /// It is from Type.AssemblyQualifiedName. THis name is used as full name in a Node
        /// It is unique for a generic type with different type of arguments.
        private readonly string fullName;

        // parent node in the class hierarchy
        private readonly INode parent; 
        
        // children in the class hierarchy
        protected IDictionary<string, INode> children = new MonotonicTreeMap<string, INode>();

        protected AbstractNode(INode parent, string name, string fullName)
        {
            this.parent = parent;
            this.name = name;
            this.fullName = fullName;
            if (parent != null)
            {
                parent.Add(this);
            }
        }

        public ICollection<INode> GetChildren()
        {
            return children.Values;
        }

        public bool Contains(string key) 
        {
            return children.ContainsKey(key);
        }

        public INode Get(string key)
        {
            INode val;
            if (children.TryGetValue(key, out val))
            {
                return val;
            }
            return null;
        }

        public virtual void Add(INode n)
        {
            children.Add(n.GetFullName(), n);
        }

        public string GetFullName()
        {
            return fullName;
        }

        public string GetName()
        {
            return name;
        }

        public INode GetParent()
        {
            return parent;
        }

        public override bool Equals(object o) 
        {
            if (o == null)
            {
                return false;
            }
            if (o == this)
            {
                return true;
            }
    
            AbstractNode n = (AbstractNode)o;
            bool parentsEqual;
            if (n.parent == this.parent) 
            {
                parentsEqual = true;
            } 
            else if (n.parent == null) 
            {
                parentsEqual = false;
            } 
            else if (this.parent == null) 
            {
                parentsEqual = false;
            } 
            else 
            {
                parentsEqual = n.parent.Equals(this.parent);
            }
            if (!parentsEqual) 
            {
                return false;
            }
            return fullName.Equals(n.fullName);
        }

        public override int GetHashCode()
        {
            return fullName.GetHashCode();
        }

        public override string ToString()
        {
            return "[" + this.GetType().FullName + " '" + fullName + "']";
        }

        public int CompareTo(INode n)
        {
            return fullName.CompareTo(n.GetFullName());
        }
    }
}