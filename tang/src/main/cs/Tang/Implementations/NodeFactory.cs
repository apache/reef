using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.Implementations
{
    public class NodeFactory
    {
        public static IPackageNode CreateRootPackageNode()
        {
            return new PackageNodeImpl();
        }

        public static INode CreateClassNode(INode parent, Type type)
        {
            var namedParameter = type.GetCustomAttribute<NamedParameterAttribute>();
            var isUnit = null != type.GetCustomAttribute<UnitAttribute>();
            string simpleName = type.Name;
            string fullName = type.FullName;
            bool isStatic = type.IsSealed && type.IsAbstract;
            bool injectable = true; // to do
            bool isAssignableFromExternalConstructor = true;//to do 

            var injectableConstructors = new List<IConstructorDef>();
            var allConstructors = new List<IConstructorDef>();

            foreach (var c in type.GetConstructors())
            {
                var isConstructorInjectable = null != c.GetCustomAttribute<InjectAttribute>();

                ConstructorDefImpl constructorDef = new ConstructorDefImpl(c.DeclaringType.FullName, isConstructorInjectable);
                foreach (var p in c.GetParameters())
                {
                    var param = p.GetCustomAttribute<ParameterAttribute>();
                    if (param != null)
                    {
                        string namedParameterName = param.GetType().FullName;
                        String ParameterTypeName = param.GetType().FullName;
                        bool isInjectionFuture = true; // TODO
                        ConstructorArgImpl arg = new ConstructorArgImpl(ParameterTypeName, namedParameterName, isInjectionFuture);
                        constructorDef.GetArgs().Add(arg);
                    }

                }

                if (isConstructorInjectable)
                {
                    injectableConstructors.Add(constructorDef);
                }
                allConstructors.Add(constructorDef);

            }

            String defaultImplementation = null;
            var defaultImpl = type.GetCustomAttribute<DefaultImplementationAttribute>();
            if (null != defaultImpl)
            {
                Type defaultImplementationClazz = defaultImpl.Value;
                defaultImplementation = defaultImplementationClazz.FullName;
            }

            return new ClassNodeImpl(parent, simpleName, fullName, isUnit, injectable, isAssignableFromExternalConstructor, injectableConstructors, allConstructors, defaultImplementation);
        }

        public static INamedParameterNode CreateNamedParameterNode(INode parent, Type type, Type argType)
        {
            return null;// TODO
        }


    }
}
