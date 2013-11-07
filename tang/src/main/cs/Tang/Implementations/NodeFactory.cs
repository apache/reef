using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;
using Com.Microsoft.Tang.Types;
using Com.Microsoft.Tang.Util;

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

                ConstructorDefImpl constructorDef = createConstructorDef(injectable, c, isConstructorInjectable);

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

        private static ConstructorDefImpl createConstructorDef(bool injectable, ConstructorInfo constructor, bool isConstructorInjectable)
        {
            var parameters = constructor.GetParameters();

            //Type[] parameterTypes = new Type[parameters.Length];
            //for (int i = 0; i < parameters.Length; i++)
            //{
            //    parameterTypes[i] = parameters[i].GetType();
            //}

            IConstructorArg[] args = new ConstructorArgImpl[parameters.Length];

            for (int i = 0; i < parameters.Length; i++)
            {
                //TODO for getInterfaceTarget() call
                Type type = parameters[i].ParameterType;
                bool isFuture = false;

                ParameterAttribute named = parameters[i].GetCustomAttribute<ParameterAttribute>();
                args[i] = new ConstructorArgImpl(type.FullName, named == null ? null : named.Value.AssemblyQualifiedName, isFuture);
            }
            return new ConstructorDefImpl(constructor.DeclaringType.FullName, args, isConstructorInjectable); 
        }

        //TimerNode, Seconds, Int32
        public static INamedParameterNode CreateNamedParameterNode(INode parent, Type type, Type argType)
        {
            Type argRawClass = ReflectionUtilities.GetRawClass(argType);

            bool isSet = false; //if it is Set TODO
           
            string simpleName = type.Name;
            string fullName = type.FullName;
            string fullArgName = argType.FullName;
            string simpleArgName = argType.Name;

            NamedParameterAttribute namedParameter = type.GetCustomAttribute<NamedParameterAttribute>();

            //if (namedParameter == null)
            //{
            //    throw new IllegalStateException("Got name without named parameter post-validation!");
            //}

            string[] defaultInstanceAsStrings = new string[]{};
            string documentation = namedParameter.Documentation;
            string shortName = namedParameter.ShortName;

            return new NamedParameterNodeImpl(parent, simpleName, fullName,
                fullArgName, simpleArgName, isSet, documentation, shortName, defaultInstanceAsStrings);
        }
    }
}
