using System;
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
        private readonly INode parent;
        private readonly String name;
        private readonly String fullName;
        protected readonly IDictionary<String, INode> children = new MonotonicTreeMap<string, INode>();


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

        public override ICollection<INode> GetChildren()
        {
            return children.Values;
        }

        public override String GetName()
        {
            return name;
        }

        public override String GetFullName()
        {
            return fullName;
        }

        public INode GetParent()
        {
            return parent;
        }

        public override bool Contains(String key) 
        {
            return children.ContainsKey(key);
        }

        public override INode Get(String key)
        {
            INode val;
            if (children.TryGetValue(key, out val))
            {
                return val;
            }
            return null;
        }

        public override void Add(INode n)
        {
            children.Add(n.GetName(), n);
        }

        public bool Equals(Object o) 
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
            return "[" + this.GetType().FullName + " '" + GetFullName() + "']";
        }

        public int CompareTo(INode n)
        {
            return GetFullName().CompareTo(n.GetFullName());
        }
    }
}
