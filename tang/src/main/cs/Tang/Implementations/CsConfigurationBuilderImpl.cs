using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Exceptions;
using Com.Microsoft.Tang.Interface;
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.Implementations
{
    public class CsConfigurationBuilderImpl : ConfigurationBuilderImpl, ICsConfigurationBuilder
    {
        public CsConfigurationBuilderImpl(string[] assemblies, IConfiguration[] confs, Type[] parsers) : base(assemblies,confs,parsers)
        {
        }

        public CsConfigurationBuilderImpl(IConfiguration[] confs) : base(confs)
        {
        }

        public CsConfigurationBuilderImpl(CsConfigurationBuilderImpl impl) : base(impl)
        {
        }
        public CsConfigurationBuilderImpl(ICsClassHierarchy classHierarchy)
            : base(classHierarchy)
        {
        }
        

        public CsConfigurationBuilderImpl(string[] assemblies)
            : base(assemblies)
        {
        }

        public CsConfigurationImpl build()
        {
            return new CsConfigurationImpl(new CsConfigurationBuilderImpl(this));
        }  
        
        private INode GetNode(Type c) 
        {
            return ((ICsClassHierarchy)ClassHierarchy).GetNode(c);
        }


        public void Bind(Type iface, Type impl)
        {
            Bind(GetNode(iface), GetNode(impl));
        }

        public void BindImplementation(Type iface, Type impl)
        {
            INode cn = GetNode(iface);
            INode dn = GetNode(impl);
            if (!(cn is IClassNode)) 
            {
                throw new BindException(
                    "bindImplementation passed interface that resolved to " + cn
                    + " expected a ClassNode<?>");
            }
            if (!(dn is IClassNode)) 
            {
                throw new BindException(
                    "bindImplementation passed implementation that resolved to " + dn
                    + " expected a ClassNode<?>");
            }
            BindImplementation((IClassNode) cn, (IClassNode) dn);
 
        }

        public void BindNamedParameter(Type name, string value)
        {
            INode np = GetNode(name);
            if (np is INamedParameterNode) 
            {
                BindParameter((INamedParameterNode)np, value);
            } 
            else 
            {
                throw new BindException(
                    "Detected type mismatch when setting named parameter " + name
                    + "  Expected NamedParameterNode, but namespace contains a " + np);
            }
        }

        public void BindNamedParameter(Type iface, Type impl)
        {
            INode ifaceN = GetNode(iface);
            INode implN = GetNode(impl);
            if (!(ifaceN is INamedParameterNode)) 
            {
                throw new BindException("Type mismatch when setting named parameter " + ifaceN
                    + " Expected NamedParameterNode");
            }
            Bind(ifaceN, implN);
        }
    }
}
