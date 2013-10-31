using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using class_hierarchy;
using Com.Microsoft.Tang.Annotations;
using Com.Microsoft.Tang.Exceptions;
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.Implementations
{
    public class ClassHierarchyImpl
    {
        INode rootNode = new PackageNodeImpl();

        public ClassHierarchyImpl(String file)
        {
            var assembly = Assembly.LoadFrom(file);
            foreach (var t in assembly.GetTypes())
            {
//                rootNode.Add(RegisterType(rootNode, t));
                RegisterType(t);
            }
        }

        public INode RegisterType(string typeFullName)
        {
            return RegisterType(Type.GetType(typeFullName));
        }

        public INode RegisterType(Type type)
        {
            try 
            {
                INode n = GetAlreadyBoundNode(type);
                return n;
            } 
            catch (NameResolutionException e) 
            {
            }

            if (type.BaseType != null)
            {
                RegisterType(type.BaseType);
            }

            foreach (Type interf in type.GetInterfaces())
            {
                RegisterType(interf);
            }

            Type enclosingClass = this.GetIEnclosingClass(type);
            if (enclosingClass != null)
            {
                RegisterType(enclosingClass);
            }

            INode node = RegisterClass(type);

            foreach (Type inner in type.GetNestedTypes())
            {
                RegisterType(inner);
            }

            IClassNode classNode = node as ClassNodeImpl;
            if (classNode != null)
            {
                foreach (IConstructorDef constructorDef in classNode.GetInjectableConstructors())
                {
                    foreach (IConstructorArg constructorArg in constructorDef.GetArgs())
                    {
                        RegisterType(constructorArg.GetType());  //GetType returns param's Type.fullname
                        if (constructorArg.GetNamedParameterName() != null)
                        {
                            NamedParameterNode np = (NamedParameterNode)RegisterType(constructorArg.GetNamedParameterName());
                            if (np != null)
                            {
                                //TODO
                            }
                            //else should throw exception

                        }
                    }
                }
            }
            else
            {
                INamedParameterNode npNode = node as INamedParameterNode;
                if (npNode != null)
                {
                    RegisterType(npNode.GetFullArgName());
                }
            }
            
            return node;
        }

        private INode RegisterClass(Type type)
        {
            INode node = GetAlreadyBoundNode(type);
            if (node != null)
            {
                return node;
            }

            node = BuildPathToNode(type);

            IClassNode classNode = node as IClassNode;
            if (classNode != null)
            {
                Type baseType = type.BaseType;
                if (baseType != null)
                {
                    try{
                        ((IClassNode)GetAlreadyBoundNode(baseType)).PutImpl(classNode);
                    }
                    catch (NameResolutionException e)
                    {
                        throw new IllegalStateException("Error in finding Node for BaseType", e);
                    }
                }

                foreach (Type interf in type.GetInterfaces())
                {
                    try
                    {
                        ((IClassNode)GetAlreadyBoundNode(interf)).PutImpl(classNode);
                    }
                    catch (NameResolutionException e)
                    {
                        throw new IllegalStateException("Error in finding Node for Interface", e);
                    }
                }
            }
            return node;
        }

        public INode BuildPathToNode(Type type)
        {
            INode parent = GetParentNode(type);
            Type argType = GetNamedParameterTargetOrNull(type);

            if (argType == null)
            {
                return CreateClassNode(parent, type);
            }
            else
            {
                CreateNamedParameterNode(parent, type, argType);
            }
            return null;
        }



        //return Type if clazz implements Name<T>, null otherwise
        public Type GetNamedParameterTargetOrNull(Type type)
        {
            return null;//TODO
        }

        public INamedParameterNode CreateNamedParameterNode(INode parent, Type type, Type argType)
        {
            return null;// TODO
        }

        public  INode CreateClassNode(INode parent, Type type)
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


            return new ClassNodeImpl(rootNode, simpleName, fullName, isUnit, injectable, isAssignableFromExternalConstructor, injectableConstructors, allConstructors, defaultImplementation);
        }

        public INode GetAlreadyBoundNode(Type t)
        {
            return null; //TODO
        }

        public INode GetParentNode(Type type)
        {
            INode current = rootNode;
            string[] enclosingPath = GetEnclosingClassNames(type);
            for (int i = 0; i < enclosingPath.Length - 1; i++)
            {
                current = current.Get(enclosingPath[i]);
            }
            return current;
        }

        public string[] GetEnclosingClassNames(Type t)
        {
            string[] path = t.FullName.Split('+');
            for (int i = 1; i < path.Length; i++)
            {
                path[i] = path[i - 1] + "." + path[i];
            }
            return path;
        }

        //return immidiate enclosing class
        public Type GetIEnclosingClass(Type t)
        {
            //get full name of t, parse it to check if there is any name before it like A+B
            //sample  t = Com.Microsoft.Tang.Examples.B+B1+B2
            //return type of Com.Microsoft.Tang.Examples.B+B1
            string[] path = GetEnclosingClassNames(t);
            if (path.Length > 1)
            {
                return Type.GetType(path[path.Length - 2]);
            }
            return null; // TODO
        }
    }
}
