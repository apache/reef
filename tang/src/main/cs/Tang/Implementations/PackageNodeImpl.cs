using System;
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.Implementations
{
    public class PackageNodeImpl : AbstractNode, IPackageNode
    {
        public PackageNodeImpl(INode parent, String name, String fullName) : 
            base(parent, name, fullName)
        {
        }

        public PackageNodeImpl()
            : base(null, "", "[root node]")
        {
        }

        /**
        * Unlike normal nodes, the root node needs to take the package name of its
        * children into account.  Therefore, we use the full name as the key when
        * we insert nodes into the root.
        */
        public void put(INode n) {
            children.Add(n.GetFullName(), n);
        }
    }
}
