using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Interface
{
    public interface ITang
    {
        IInjector NewInjector();
        IInjector NewInjector(IConfiguration[] confs);
        IInjector NewInjector(IConfiguration confs);
        IClassHierarchy GetClassHierarchy(string assembly);
        ICsClassHierarchy GetDefaultClassHierarchy();
        ICsClassHierarchy GetDefaultClassHierarchy(string[] assemblies, Type[] parameterParsers);

        ICsConfigurationBuilder NewConfigurationBuilder();
        ICsConfigurationBuilder NewConfigurationBuilder(string[] assemblies);
        ICsConfigurationBuilder NewConfigurationBuilder(IConfiguration[] confs);
        ICsConfigurationBuilder NewConfigurationBuilder(string[] assemblies, IConfiguration[] confs, Type[] parameterParsers);
        ICsConfigurationBuilder NewConfigurationBuilder(ICsClassHierarchy classHierarchy);
    }
}
