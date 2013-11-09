using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.Interface
{
    public interface IConfiguration
    {
        IList<IClassNode> GetBoundImplementations();
        
        IList<IClassNode> GetBoundConstructors();

        IList<IClassNode> GetNamedParameters();
    }
}
