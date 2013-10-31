using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Types
{
    public interface IClassNode : INode
    {
        IList<IConstructorDef> GetInjectableConstructors();
        IConstructorDef GetConstructorDef(IList<IClassNode> args);
        IList<IConstructorDef> GetAllConstructors();

        void PutImpl(IClassNode impl);
        ISet<IClassNode> GetKnownImplementations();
        String GetDefaultImplementation();
        bool IsUnit();
        bool IsInjectionCandidate();
        bool IsExternalConstructor();
        bool IsImplementationOf(IClassNode inter);
    }
}
