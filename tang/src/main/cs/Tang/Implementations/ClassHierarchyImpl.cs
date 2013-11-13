using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;
using Com.Microsoft.Tang.Exceptions;
using Com.Microsoft.Tang.Interface;
using Com.Microsoft.Tang.Types;
using Com.Microsoft.Tang.Util;

namespace Com.Microsoft.Tang.Implementations
{
    public class ClassHierarchyImpl : IClassHierarchy
    {
        private INode rootNode = NodeFactory.CreateRootPackageNode();
        private MonotonicTreeMap<String, INamedParameterNode> shortNames = new MonotonicTreeMap<String, INamedParameterNode>();
        public Assembly assembly { get; private set; }

        public ClassHierarchyImpl(String file)
        {
            assembly = Assembly.LoadFrom(file);
            foreach (var t in assembly.GetTypes())
            {
                RegisterType(t);
            }
        }

        public INode RegisterType(string assemblyQualifiedName)
        {
            Type type = Type.GetType(assemblyQualifiedName);
            if (type != null)
            {
                return RegisterType(Type.GetType(assemblyQualifiedName));
            }
            return null;
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
                        if (constructorArg.Gettype() == null)
                        {
                            throw new ArgumentException("not type in arg");
                        }
                        RegisterType(constructorArg.Gettype());  //Gettype returns param's Type.fullname
                        if (constructorArg.GetNamedParameterName() != null)
                        {
                            INamedParameterNode np = (INamedParameterNode)RegisterType(constructorArg.GetNamedParameterName());
                            if (np.IsSet())
                            {
                                throw new NotImplementedException();
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
            try
            {
                return  GetAlreadyBoundNode(type);
            }
            catch(NameResolutionException e)
            {
            }

            INode node = BuildPathToNode(type);

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

        //return Type T if type implements Name<T>, null otherwise
        //e.g. [NamedParameter(typeof(System.String), "Number of seconds to sleep", "10", "sec")]
        //class Seconds : Name<Int32> { }
        //return Int32
        public Type GetNamedParameterTargetOrNull(Type type)
        {
            var npAnnotation = type.GetCustomAttribute<NamedParameterAttribute>();
            if (npAnnotation != null)
            {
                Type[] intfs = type.GetInterfaces();
                if (intfs.Length == 1)
                {
                    if (intfs[0].Name.Equals(GetNameOfNameInterface()))
                    {
                        Type[] args = intfs[0].GetGenericArguments();
                        if (args.Length == 1)
                        {
                            return args[0];
                        }
                    }
                }

            }
            return null;   
        }

        private INode GetAlreadyBoundNode(Type t)
        {
            string[] outerClassNames = GetEnclosingClassShortNames(t);
            string outerClassName = outerClassNames[0];
            INode current = rootNode.Get(outerClassName);

            if (current == null)
            {
                throw new NameResolutionException(t.FullName, outerClassName);
            }

            for (int i = 1; i < outerClassNames.Length; i++)            
            {
                current = current.Get(outerClassNames[i]);
                if (current == null)
                {
                    StringBuilder sb = new StringBuilder(outerClassName);
                    for (int j = 0; j < i; j++)
                    {
                        sb.Append(outerClassNames[j]);
                        if (j != i - 1)
                        {
                            sb.Append(".");
                        }
                    }
                    throw new NameResolutionException(t.FullName, sb.ToString());
                }

            }
            return current; 
        }

        //starting from the root, get child for each eclosing class excluding the type itsself
        //all enclosing classes should be already in the hierarchy
        private INode GetParentNode(Type type)
        {
            INode current = rootNode;
            string[] enclosingPath = GetEnclosingClassShortNames(type);
            for (int i = 0; i < enclosingPath.Length - 1; i++)
            {
                current = current.Get(enclosingPath[i]);
            }
            return current;
        }

        //first name is with name space, rest of them are simple names only
        private string[] GetEnclosingClassNames(Type t)
        {
            string[] path = t.FullName.Split('+');
            return path;
        }

        private string[] GetEnclosingClassFullNames(Type t)
        {
            string[] path = t.FullName.Split('+');
            for (int i = 1; i < path.Length; i++)
            {
                path[i] = path[i - 1] + "." + path[i];
            }
            return path;
        }

        //return all parent class names including itself
        private string[] GetEnclosingClassShortNames(Type t)
        {
            string[] path = t.FullName.Split('+');

            if (path.Length == 1)
            {
                return new string[1] { t.Name };
            }
            string[] first = path[0].Split('.');
            path[0] = first[first.Length - 1];

            return path;
        }

        //return immidiate enclosing class
        private Type GetIEnclosingClass(Type t)
        {
            //get full name of t, parse it to check if there is any name before it like A+B
            //sample  t = Com.Microsoft.Tang.Examples.B+B1+B2
            //return type of Com.Microsoft.Tang.Examples.B+B1
            string[] path = GetEnclosingClassFullNames(t);
            if (path.Length > 1)
            {
                return Type.GetType(path[path.Length - 2]);
            }
            return null; // TODO
        }

        private string GetNameOfNameInterface()
        {
            var tn = typeof(Name<int>);
            return tn.Name;
        }

        public INode GetNode(string fullName)
        {
            Type t = Type.GetType(fullName);
            if (t == null)
            {
                t = this.assembly.GetType(fullName);
            }

            if (t == null)
            {
                throw new NameResolutionException(fullName, fullName);
            }
            return this.GetNode(t);
        }

        public INode GetNode(Type type)
        {
            INode current = rootNode;
            string[] enclosingPath = GetEnclosingClassShortNames(type);
            for (int i = 0; i < enclosingPath.Length; i++)
            {
                current = current.Get(enclosingPath[i]);
            }
            return current;
        }

        public INode GetNamespace()
        {
            return rootNode;
        }

        public bool IsImplementation(IClassNode inter, IClassNode impl)
        {
            throw new NotImplementedException();
        }

        public IClassHierarchy Merge(IClassHierarchy ch)
        {
            throw new NotImplementedException();
        }
    }
}
