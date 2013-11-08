using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Interface
{
    public interface IConfigurationBuilder
    {
        IClassHierarchy GetClassHierarchy();
        void Bind(string iface, string impl);
        void BindNamedParameter(Type name, string value);  //name must extend from Name<T>
        void bindImplementation(Type iface, Type impl); //impl must implement iFace
    }
}
