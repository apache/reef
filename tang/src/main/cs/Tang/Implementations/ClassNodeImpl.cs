using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
            this.knownImpls = new MonotonicSet<>();
            this.defaultImpl = defaultImplementation;
        }


        public override IConstructorDef<T>[] GetAllConstructors() {
            return allConstructors;
        }

        public override bool IsInjectionCandidate() 
        {
            return injectable;
        }

        public override bool IsExternalConstructor() 
        {
            return externalConstructor;
        }
    }
}
