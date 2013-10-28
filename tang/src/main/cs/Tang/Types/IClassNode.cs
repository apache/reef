using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Types
{
    public interface IClassNode<T> : INode
    {
        IConstructorDef<T>[] GetInjectableConstructors();
        IConstructorDef<T> GetConstructorDef(IList<IClassNode<T>> args);
        IConstructorDef<T>[] GetAllConstructors();

        void PutImpl(IClassNode<T> impl);
        ISet<IClassNode<T>> GetKnownImplementations();
        String GetDefaultImplementation();
        bool IsUnit();
        bool IsInjectionCandidate();
        bool IsExternalConstructor();
        bool IsImplementationOf(IClassNode<T> inter);
    }
}
