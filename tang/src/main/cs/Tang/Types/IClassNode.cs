using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Types
{
    public interface IClassNode<T> : INode
    {
        IConstructorDef<T>[] getInjectableConstructors();
        IConstructorDef<T> getConstructorDef(params IClassNode<T>[] args);
        IConstructorDef<T>[] getAllConstructors();

        void PutImpl(IClassNode<T> impl);
        ISet<IClassNode<T>> GetKnownImplementations();
        String GetDefaultImplementation();
        bool IsUnit();
        bool IsInjectionCandidate();
        bool IsExternalConstructor();
        bool IsImplementationOf(IClassNode<T> inter);
    }
}
