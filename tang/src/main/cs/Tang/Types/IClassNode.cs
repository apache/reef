using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Types
{
    public interface IClassNode<T> : INode
    {
        ConstructorDef<T>[] getInjectableConstructors();
        ConstructorDef<T> getConstructorDef(params IClassNode<T>[] args);
        ConstructorDef<T>[] getAllConstructors();

        void PutImpl(IClassNode<T> impl);
        ISet<IClassNode<T>> GetKnownImplementations();
        String GetDefaultImplementation();
        bool IsUnit();
        bool IsInjectionCandidate();
        bool IsExternalConstructor();
        bool IsImplementationOf(IClassNode<T> inter);
    }
}
