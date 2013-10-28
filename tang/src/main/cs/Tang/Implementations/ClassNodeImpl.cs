using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Exceptions;
using Com.Microsoft.Tang.Types;
using Com.Microsoft.Tang.Util;

namespace Com.Microsoft.Tang.Implementations
{
    public class ClassNodeImpl<T> : AbstractNode, IClassNode<T>
    {
        private readonly bool injectable;
        private readonly bool unit;
        private readonly bool externalConstructor;
        private readonly IConstructorDef<T>[] injectableConstructors;
        private readonly IConstructorDef<T>[] allConstructors;
        private readonly MonotonicSet<IClassNode<T>> knownImpls;
        private readonly String defaultImpl;

        public ClassNodeImpl(INode parent, String simpleName, String fullName,
            bool unit, bool injectable, bool externalConstructor,
            IConstructorDef<T>[] injectableConstructors,
            IConstructorDef<T>[] allConstructors,
            String defaultImplementation) : base(parent, simpleName, fullName) 
        {

            this.unit = unit;
            this.injectable = injectable;
            this.externalConstructor = externalConstructor;
            this.injectableConstructors = injectableConstructors;
            this.allConstructors = allConstructors;
            this.knownImpls = new MonotonicSet<IClassNode<T>>();
            this.defaultImpl = defaultImplementation;
        }

        public IConstructorDef<T>[] GetInjectableConstructors()
        {
            return injectableConstructors;
        }

        public IConstructorDef<T>[] GetAllConstructors() {
            return allConstructors;
        }

        public bool IsInjectionCandidate() 
        {
            return injectable;
        }

        public bool IsExternalConstructor() 
        {
            return externalConstructor;
        }

        public override String ToString() 
        {
            StringBuilder sb = new StringBuilder(base.ToString() + ": ");
            if (GetInjectableConstructors() != null) 
            {
                foreach (IConstructorDef<T> c in GetInjectableConstructors()) 
                {
                    sb.Append(c.ToString() + ", ");
                }
            } 
            else 
            {
                sb.Append("OBJECT BUILD IN PROGRESS!  BAD NEWS!");
            }
            return sb.ToString();
        }

        public IConstructorDef<T> GetConstructorDef(IList<IClassNode<T>> paramTypes)
        {
            if (!IsInjectionCandidate()) 
            {
                throw new BindException("Cannot @Inject non-static member/local class: "
                + GetFullName());
            }
            foreach (IConstructorDef<T> c in GetAllConstructors())
            {
                if (c.TakesParameters(paramTypes)) 
                {
                    return c;
                }
            }
            throw new BindException("Could not find requested constructor for class "
                + GetFullName());
        }

        public void PutImpl(IClassNode<T> impl)
        {
            knownImpls.Add(impl);
        }

        public ISet<IClassNode<T>> GetKnownImplementations() 
        {
            return new MonotonicSet<IClassNode<T>>(knownImpls);
        }

        public bool IsUnit()
        {
            return unit;
        }

        public bool IsImplementationOf(IClassNode<T> inter) 
        {
            IList<IClassNode<T>> worklist = new List<IClassNode<T>>();
            if (this.Equals(inter)) {
                return true;
            }
            worklist.Add(inter);
            while (worklist.Count != 0) 
            {
                IClassNode<T> cn = worklist[worklist.Count - 1];
                worklist.RemoveAt(worklist.Count - 1);
                ISet<IClassNode<T>> impls = cn.GetKnownImplementations();
                if (impls.Contains(this)) 
                {
                    return true;
                }
                worklist.Concat(impls);
            }
            return false;
        }

        public String GetDefaultImplementation()
        {
            return defaultImpl;
        }
    }
}
