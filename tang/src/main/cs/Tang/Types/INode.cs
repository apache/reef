using System;

namespace Com.Microsoft.Tang.Types
{
    public interface INode : IComparable<INode>, ITraversable<INode> 
    {
        
        string GetName();

        string GetFullName();

        bool Contains(string key);

        INode Get(string key);

        INode GetParent();

        void Add(INode node);
    }
}
