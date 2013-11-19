using System;
using System.Collections.Generic;

namespace Com.Microsoft.Tang.Types
{
    public interface IClassNode : INode
    {
        IList<IConstructorDef> GetInjectableConstructors();
        IConstructorDef GetConstructorDef(IList<INode> args);
        IList<IConstructorDef> GetAllConstructors();

        void PutImpl(IClassNode impl);
        ISet<IClassNode> GetKnownImplementations();
        string GetDefaultImplementation();
        bool IsUnit();
        bool IsInjectionCandidate();
        bool IsExternalConstructor();
        bool IsImplementationOf(IClassNode inter);
    }
}
