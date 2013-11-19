using System;
using System.Collections.Generic;

namespace Com.Microsoft.Tang.Types
{
    public interface IConstructorDef : IComparable
    {
        string GetClassName();

        IList<IConstructorArg> GetArgs();

        bool IsMoreSpecificThan(IConstructorDef def);

        bool TakesParameters(IList<INode> paramTypes);
    }
}
