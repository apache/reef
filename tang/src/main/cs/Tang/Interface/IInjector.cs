using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Implementations;

namespace Com.Microsoft.Tang.Interface
{
    public interface IInjector
    {
        /// <summary>
        /// Gets an instance of iface, or the implementation that has been bound to it.
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <returns></returns>
        object GetInstance(Type iface);

        object GetInstance(string iface);

        object GetNamedInstance(Type iface); //iface must implement Name<T>
        
        InjectionPlan GetInjectionPlan(Type name);

        bool IsInjectable(string name);

        bool IsParameterSet(String name);

        bool IsInjectable(Type clazz);

        bool isParameterSet(Type name);
    }
}
