using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Interface;

namespace Com.Microsoft.Tang.Implementations
{
    public class ConfigurationBuilderImpl : IConfigurationBuilder
    {

        public IClassHierarchy GetClassHierarchy()
        {
            throw new NotImplementedException();
        }

        public void Bind(string iface, string impl)
        {
            throw new NotImplementedException();
        }

        public void BindNamedParameter(Type name, string value)
        {
            throw new NotImplementedException();
        }

        public void BindImplementation(Type iface, Type impl)
        {
            throw new NotImplementedException();
        }


        public IConfiguration Build()
        {
            throw new NotImplementedException();
        }

        public void Bind(Types.INode key, Types.INode value)
        {
            throw new NotImplementedException();
        }

        public void BindNamedParameter(Type iface, Type impl)
        {
            throw new NotImplementedException();
        }
    }
}
