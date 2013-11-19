using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.Interface
{
    public interface ICsClassHierarchy : IClassHierarchy
    {
        INode GetNode(Type c);
        Type ClassForName(string name);
        object Parse(INamedParameterNode name, string value);
        object ParseDefaultValue(INamedParameterNode name);


    }
}
