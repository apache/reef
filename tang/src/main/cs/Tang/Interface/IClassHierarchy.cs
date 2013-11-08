using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.Interface
{
    public interface IClassHierarchy
    {
        INode GetNode(string fullName);
        INode GetNamespace();
        bool IsImplementation(IClassNode inter, IClassNode impl);
        IClassHierarchy merge(IClassHierarchy ch);
    }
}
