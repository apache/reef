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
using Com.Microsoft.Tang.Util;

namespace Com.Microsoft.Tang.Implementations
{
    public class ClassHierarchyImpl
    {
        private INode rootNode = NodeFactory.CreateRootPackageNode();
        private MonotonicTreeMap<String, INamedParameterNode> shortNames = new MonotonicTreeMap<String, INamedParameterNode>();

        public ClassHierarchyImpl(String file)
        {
            var assembly = Assembly.LoadFrom(file);
            foreach (var t in assembly.GetTypes())
            {
                RegisterType(t);
            }
        }

        public INode RegisterType(string assemblyQualifiedName)
        {
            return RegisterType(Type.GetType(assemblyQualifiedName));
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
                            INamedParameterNode np = (INamedParameterNode)RegisterType(constructorArg.GetNamedParameterName());
                            if (np.IsSet())
                            {
                                //TODO
                            }
                            else
                            {
                                //check is not isCoercable, then throw ClassHierarchyException
                            }
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
                return NodeFactory.CreateClassNode(parent, type);
            }
            else
            {
                INamedParameterNode np = NodeFactory.CreateNamedParameterNode(parent, type, argType);

                //TODO
                //if(parameterParser.canParse(ReflectionUtilities.getFullName(argType))) {
                //    if(clazz.getAnnotation(NamedParameter.class).default_class() != Void.class) {
                //      throw new ClassHierarchyException("Named parameter " + ReflectionUtilities.getFullName(clazz) + " defines default implementation for parsable type " + ReflectionUtilities.getFullName(argType));
                //    }
                //}

                string shortName = np.GetShortName();
                if (shortName != null)
                {
                    INamedParameterNode oldNode = null;
                    shortNames.TryGetValue(shortName, out oldNode);
                    if (oldNode != null)
                    {
                        if (oldNode.GetFullName().Equals(np.GetFullName()))
                        {
                            throw new IllegalStateException("Tried to double bind "
                                + oldNode.GetFullName() + " to short name " + shortName);
                        }
                        throw new ClassHierarchyException("Named parameters " + oldNode.GetFullName()
                            + " and " + np.GetFullName() + " have the same short name: "
                            + shortName);
                    }
                    shortNames.Add(shortName, np);

                }
                return np;
            }
        }



        //return Type if clazz implements Name<T>, null otherwise
        public Type GetNamedParameterTargetOrNull(Type type)
        {
            return null;//TODO
        }

        public INode GetAlreadyBoundNode(Type t)
        {
            INode current = rootNode;
            foreach (string outClassName in GetEnclosingClassNames(t))
            {
                current = current.Get(outClassName);
                if (current == null)
                {
                    StringBuilder sb = new StringBuilder(outClassName);

                }
            }
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
