using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.Interface
{
    public interface IConfigurationBuilder
    {
        IClassHierarchy GetClassHierarchy();
        IConfiguration Build();
        void Bind(string iface, string impl);
        void Bind(INode key, INode value);
        void BindNamedParameter(Type name, string value);  //name must extend from Name<T>
        void BindNamedParameter(Type iface, Type impl); // iface must extend from Name<T> and impl extend from T
        void BindImplementation(Type iface, Type impl); //impl must implement iface
    }
}
