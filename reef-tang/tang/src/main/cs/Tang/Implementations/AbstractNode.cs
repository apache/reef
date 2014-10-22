/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Types;
using Com.Microsoft.Tang.Util;

namespace Com.Microsoft.Tang.Implementations
{
    public class AbstractNode : INode
    {
        protected INode parent;
        protected String name;
        protected String fullName;
        protected IDictionary<String, INode> children = new MonotonicTreeMap<string, INode>();


        public AbstractNode(INode parent, String name, String fullName)
        {
            this.parent = parent;
            this.name = name;
            this.fullName = fullName;
            if (parent != null)
            {
                if (name.Length == 0)
                {
                    throw new ArgumentException(
                        "Zero length child name means bad news");
                }
                parent.Add(this);
            }
        }

        public ICollection<INode> GetChildren()
        {
            return children.Values;
        }

        public bool Contains(String key) 
        {
            return children.ContainsKey(key);
        }

        public INode Get(String key)
        {
            INode val;
            if (children.TryGetValue(key, out val))
            {
                return val;
            }
            return null;
        }

        public void Add(INode n)
        {
            children.Add(n.GetName(), n);
        }

        public string GetFullName()
        {
            return this.fullName;
        }

        public string GetName()
        {
            return this.name;
        }

        public INode GetParent()
        {
            return this.parent;
        }


        public override bool Equals(Object o) 
        {
            if(o == null) return false;
            if(o == this) return true;
    
            AbstractNode n = (AbstractNode) o;
            bool parentsEqual;
            if (n.parent == this.parent) {
                parentsEqual = true;
            } else if (n.parent == null) {
                parentsEqual = false;
            } else if (this.parent == null) {
                parentsEqual = false;
            } else {
                parentsEqual = n.parent.Equals(this.parent);
            }
            if (!parentsEqual) {
                return false;
            }
            return this.name.Equals(n.name);
        }

        public override String ToString()
        {
            return "[" + this.GetType().FullName + " '" + fullName + "']";
        }

        public int CompareTo(INode n)
        {
            return fullName.CompareTo(n.GetFullName());
        }
    }
}
