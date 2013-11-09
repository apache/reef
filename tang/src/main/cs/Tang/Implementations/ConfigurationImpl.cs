using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Interface;

namespace Com.Microsoft.Tang.Implementations
{
    class ConfigurationImpl : IConfiguration
    {
        public IList<Types.IClassNode> GetBoundImplementations()
        {
            throw new NotImplementedException();
        }

        public IList<Types.IClassNode> GetBoundConstructors()
        {
            throw new NotImplementedException();
        }

        public IList<Types.IClassNode> GetNamedParameters()
        {
            throw new NotImplementedException();
        }
    }
}
