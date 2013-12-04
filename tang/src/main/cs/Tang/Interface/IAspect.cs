using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Implementations;
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.Interface
{
    public interface Aspect
    {
        object Inject(IConstructorDef def, ConstructorInfo constructor, object[] args);
        void InjectionFutureInstantiated(InjectionFuture f, object t);
        Aspect CreateChildAspect();
    }
}
