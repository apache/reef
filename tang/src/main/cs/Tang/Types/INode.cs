using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Types
{
    public interface INode : IComparable<INode>, ITraversable<INode> 
    {
        
        String GetName();

        String GetFullName();

        bool Contains(String key);

        INode Get(String key);

        INode GetParent();

        void put(INode node);

        String toString();
    }
}
