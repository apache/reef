using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Interface;
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.Implementations
{
    public class ConfigurationImpl : IConfiguration
    {
        public readonly ConfigurationBuilderImpl Builder;

        public ConfigurationImpl(ConfigurationBuilderImpl builder)
        {
            this.Builder = builder;
        }

        public IClassHierarchy GetClassHierarchy()
        {
            return Builder.ClassHierarchy;
        }

        public IConfigurationBuilder newBuilder()
        {
            //create a new builder using the current Builder, and craete a new COnfiguration that wraps teh builder.
            IConfiguration c = new ConfigurationBuilderImpl(Builder).Build();
            return ((ConfigurationImpl)c).Builder;
            //return ((ConfigurationImpl)Builder.Build()).Builder;
        }
           
        public ICollection<IClassNode> GetBoundImplementations()
        {
            return Builder.BoundImpls.Keys;
        }

        public IClassNode GetBoundImplementation(Types.IClassNode cn)
        {
            IClassNode v;

            Builder.BoundImpls.TryGetValue(cn, out v);

            return v;
        }

        public ICollection<IClassNode> GetBoundConstructors()
        {
            return Builder.BoundConstructors.Keys;
        }

        public IClassNode GetBoundConstructor(Types.IClassNode cn)
        {
            IClassNode v;

            Builder.BoundConstructors.TryGetValue(cn, out v);

            return v;
        }

        public ICollection<INamedParameterNode> GetNamedParameters()
        {
            return Builder.NamedParameters.Keys;
        }

        public string GetNamedParameter(INamedParameterNode np)
        {
            string v = null;
            Builder.NamedParameters.TryGetValue(np, out v);

            return v;
        }

        public IConstructorDef GetLegacyConstructor(IClassNode cn)
        {
            IConstructorDef v;

            Builder.LegacyConstructors.TryGetValue(cn, out v);

            return v;
        }

        public ICollection<IClassNode> GetLegacyConstructors()
        {
            return Builder.LegacyConstructors.Keys;
        }

        public ISet<Object> GetBoundSet(INamedParameterNode np) 
        {
            return new HashSet<object>();
            //TODO
            //return this.Builder.BoundSetEntries.getValuesForKey(np);
        }
    }
}
