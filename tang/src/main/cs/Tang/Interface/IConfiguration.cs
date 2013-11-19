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
        IConfigurationBuilder newBuilder();
        string GetNamedParameter(INamedParameterNode np);
        IClassHierarchy GetClassHierarchy();

        ISet<Object> GetBoundSet(INamedParameterNode np); //named parameter for a set

        IClassNode GetBoundConstructor(IClassNode cn);
        IClassNode GetBoundImplementation(IClassNode cn);
        IConstructorDef GetLegacyConstructor(IClassNode cn);

        ICollection<IClassNode> GetBoundImplementations();
        ICollection<IClassNode> GetBoundConstructors();
        ICollection<INamedParameterNode> GetNamedParameters();
        ICollection<IClassNode> GetLegacyConstructors();
    }
}
