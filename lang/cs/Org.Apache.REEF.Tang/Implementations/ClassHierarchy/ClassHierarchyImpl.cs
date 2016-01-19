// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Implementations.ClassHierarchy
{
    public class ClassHierarchyImpl : ICsClassHierarchy
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ClassHierarchyImpl));
        private readonly INode rootNode;
        private readonly MonotonicTreeMap<string, INamedParameterNode> shortNames = new MonotonicTreeMap<string, INamedParameterNode>();
        private readonly IList<string> assemblies;
        private readonly AssemblyLoader loader = null;
        private object _nodeLock = new object();
        private object _mergeLock = new object();
        private object _implLock = new object();

        // alias is indexed by language, for each language, a mapping between alias and corresponding name kept in a Dictionary
        private readonly IDictionary<string, IDictionary<string, string>> _aliasLookupTable = new Dictionary<string, IDictionary<string, string>>();

        public ParameterParser Parameterparser = new ParameterParser();

        public ClassHierarchyImpl(string file) : this(new string[] { file }, new Type[0])
        {
        }

        public ClassHierarchyImpl(string[] assemblies) : this(assemblies, new Type[0])
        {
        }

        // parameterParsers are classes that extends from IExternalConstructor
        public ClassHierarchyImpl(string[] assemblies, Type[] parameterParsers)  
        {
            this.assemblies = assemblies;
            rootNode = NodeFactory.CreateRootPackageNode();
            loader = new AssemblyLoader(assemblies);
           
            foreach (Type p in parameterParsers)
            {
                // p must be extend from IExternalConstructor
                try 
                {
                    Parameterparser.AddParser(p);
                } 
                catch (BindException e)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ArgumentException("Could not register parameter parsers", e), LOGGER);
                }
            }

            foreach (var a in loader.Assemblies)
            {
                Type[] types;
                try
                {
                    types  = a.GetTypes();
                }
                catch (ReflectionTypeLoadException exception)
                {
                    LOGGER.Log(Level.Warning,
                        "GetTypes failed for assembly: {0} LoaderExceptions: {1}",
                        a,
                        string.Join<Exception>("; ", exception.LoaderExceptions));
                    continue;
                }

                foreach (var t in types)
                {
                    try
                    {
                        RegisterType(t);
                    }
                    catch (FileNotFoundException exception)
                    {
                        LOGGER.Log(Level.Warning, "Could not register type: {0} Exception: {1}", t, exception);
                    }
                }
            }
        }

        public INode RegisterType(string assemblyQualifiedName)
        {
            Type type = this.loader.GetType(assemblyQualifiedName);
            if (type != null)
            {
                return RegisterType(type);
            }
            return null;
        }

        public INode RegisterType(Type type)
        {
            if (ReflectionUtilities.IsAnonymousType(type))
            {
                // DevNote: Kinda hacky way to indicate the no-op case.
                return rootNode;
            }

            INode n = GetAlreadyBoundNode(type);
            if (n != null)
            {
                return n;
            }

            if (type.BaseType != null)
            {
                RegisterType(type.BaseType);
            }

            foreach (Type interf in type.GetInterfaces())
            {
                RegisterType(ReflectionUtilities.EnsureInterfaceType(interf));
            }

            Type enclosingClass = type.DeclaringType; // this.GetEnclosingClass(type);
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
                            Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ArgumentException("not type in arg"), LOGGER);
                        }
                        RegisterType(constructorArg.Gettype());  // Gettype returns param's Type.fullname
                        if (constructorArg.GetNamedParameterName() != null)
                        {
                            INamedParameterNode np = (INamedParameterNode)RegisterType(constructorArg.GetNamedParameterName());
                            try
                            {
                                if (np.IsSet() || np.IsList())
                                {
                                    // throw new NotImplementedException();
                                }
                                else
                                {
                                    if (!ReflectionUtilities.IsCoercable(ClassForName(constructorArg.Gettype()), ClassForName(np.GetFullArgName())))
                                    {
                                        var e = new ClassHierarchyException(
                                            "Named parameter type mismatch in " + classNode.GetFullName() + ".  Constructor expects a "
                                                + constructorArg.Gettype() + " but " + np.GetName() + " is a "
                                                + np.GetFullArgName());
                                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                                    }
                                }
                            }
                            catch (TypeLoadException e)
                            {
                                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                                var ex = new ClassHierarchyException("Constructor refers to unknown class "
                                    + constructorArg.GetType(), e);
                                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
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
                    IClassNode n = (IClassNode)GetAlreadyBoundNode(baseType);
                    if (n != null)
                    {
                        n.PutImpl(classNode);
                    }
                    else
                    {
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException("Error in finding Node for BaseType"), LOGGER);
                    }
                }

                foreach (Type interf in ReflectionUtilities.GetInterfaces(type, false))
                {
                    IClassNode n = (IClassNode)GetAlreadyBoundNode(ReflectionUtilities.EnsureInterfaceType(interf));
                    if (n != null)
                    {
                        n.PutImpl(classNode);
                    }
                    else
                    {
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException("Error in finding Node for Interface"), LOGGER);
                    }
                }
            }
            return node;
        }

        public INode BuildPathToNode(Type type)
        {
            INode parent = GetParentNode(type);

            Type argType = ReflectionUtilities.GetNamedParameterTargetOrNull(type);

            if (argType == null)
            {
                return NodeFactory.CreateClassNode(parent, type);
            }
            INamedParameterNode np = NodeFactory.CreateNamedParameterNode(parent, type, argType);

            if (Parameterparser.CanParse(ReflectionUtilities.GetAssemblyQualifiedName(argType))) 
            {
                if (type.GetCustomAttribute<NamedParameterAttribute>().DefaultClass != null) 
                {
                    var e = new ClassHierarchyException("Named parameter " + ReflectionUtilities.GetAssemblyQualifiedName(type) + " defines default implementation for parsable type " + ReflectionUtilities.GetAssemblyQualifiedName(argType));
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
            }

            if (!string.IsNullOrEmpty(np.GetAlias()))
            {
                IDictionary<string, string> mapping = null;
                _aliasLookupTable.TryGetValue(np.GetAliasLanguage().ToString(), out mapping);
                if (null == mapping)
                {
                    mapping = new Dictionary<string, string>();
                    _aliasLookupTable.Add(np.GetAliasLanguage().ToString(), mapping);
                }
                try
                {
                    mapping.Add(np.GetAlias(), np.GetFullName());
                }
                catch (Exception)
                {
                    var e = new ApplicationException(string.Format(CultureInfo.CurrentCulture, "Duplicated alias {0} on named parameter {1}.", np.GetAlias(), np.GetFullName()));
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
            }

            string shortName = np.GetShortName();
            if (shortName != null && !shortName.Equals(string.Empty))
            {
                INamedParameterNode oldNode = null;
                shortNames.TryGetValue(shortName, out oldNode);
                if (oldNode != null)
                {
                    if (oldNode.GetFullName().Equals(np.GetFullName()))
                    {
                        var ex = new IllegalStateException("Tried to double bind "
                            + oldNode.GetFullName() + " to short name " + shortName);
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                    }
                    var e = new ClassHierarchyException("Named parameters " + oldNode.GetFullName()
                        + " and " + np.GetFullName() + " have the same short name: "
                        + shortName);
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
                shortNames.Add(shortName, np);
            }
            return np;            
        }

        // return Type T if type implements Name<T>, null otherwise
        // e.g. [NamedParameter(typeof(System.String), "Number of seconds to sleep", "10", "sec")]
        // class Seconds : Name<Int32> { }
        // return Int32

        // TODO add error handlings
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
            // get outclass names including itsself
            string[] outerClassNames = ReflectionUtilities.GetEnclosingClassNames(t);

            INode current = rootNode;
            for (int i = 0; i < outerClassNames.Length; i++)            
            {
                current = current.Get(outerClassNames[i]);
                if (current == null)
                {
                    StringBuilder sb = new StringBuilder();
                    for (int j = 0; j <= i; j++)
                    {
                        sb.Append(outerClassNames[j]);
                        if (j != i)
                        {
                            sb.Append(".");
                        }
                    }
                    return null;
                    //// throw new NameResolutionException(t.FullName, sb.ToString());
                }
            }
            return current; 
        }

        // starting from the root, get child for each eclosing class excluding the type itsself
        // all enclosing classes should be already in the hierarchy
        // Type B2 = asm.GetType(@"Org.Apache.REEF.Tang.Examples.B+B1+B2");
        // string[] pathB2 = ClassNameParser.GetEnclosingClassShortNames(B2);
        // Assert.AreEqual(pathB2[0], "B");
        // Assert.AreEqual(pathB2[1], "B1");
        // Assert.AreEqual(pathB2[2], "B2");
        // return INode for B1
        private INode GetParentNode(Type type)
        {
            INode current = rootNode;
            string[] enclosingPath = ReflectionUtilities.GetEnclosingClassNames(type);
            for (int i = 0; i < enclosingPath.Length - 1; i++)
            {
                current = current.Get(enclosingPath[i]);
            }
            return current;
        }

        private string GetNameOfNameInterface()
        {
            var tn = typeof(Name<int>);
            return tn.Name;
        }

        public INode GetNode(string fullName)
        {
            Type t = loader.GetType(fullName);

            if (t == null)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new NameResolutionException(fullName, fullName), LOGGER);
            }

            return this.GetNode(t);
        }

        /// <summary>
        /// This method get INode from the class hierarchy by fullName. 
        /// If the Type for the name is not found in assemblies, it will found alias for aliasLanguage. 
        /// If alias is found, it will get the Type for the alias then call GetNode(Type). 
        /// </summary>
        /// <param name="fullName"></param>
        /// <param name="aliasLanguage"></param>
        /// <returns></returns>
        public INode GetNode(string fullName, string aliasLanguage)
        {
            Type t = null;
            try
            {
                t = loader.GetType(fullName);
            }
            catch (ApplicationException)
            {
                IDictionary<string, string> mapping = null;
                _aliasLookupTable.TryGetValue(aliasLanguage, out mapping);
                if (mapping != null)
                {
                    string assemblyName;
                    mapping.TryGetValue(fullName, out assemblyName);
                    if (assemblyName != null)
                    {
                        t = loader.GetType(assemblyName);
                    }
                    else
                    {
                        t = null;
                    }
                }
                else
                {
                    t = null;
                }
            }

            if (t == null)
            {
               Utilities.Diagnostics.Exceptions.Throw(new NameResolutionException(fullName, fullName), LOGGER);
            }

            return this.GetNode(t);
        }

        public INode GetNode(Type type)
        {
            lock (_nodeLock)
            {
                RegisterType(type);
                return GetAlreadyBoundNode(type);
            }
        }

        public INode GetNamespace()
        {
            return rootNode;
        }

        public bool IsImplementation(IClassNode inter, IClassNode impl)
        {
            lock (_implLock)
            {
                return impl.IsImplementationOf(inter);
            }
        }

        public IClassHierarchy Merge(IClassHierarchy ch)
        {
            if (this == ch) 
            { 
                return this; 
            }

            if (!(ch is ClassHierarchyImpl)) 
            {
                Utilities.Diagnostics.Exceptions.Throw(new NotSupportedException("Can't merge java and non-java class hierarchies yet!"), LOGGER);
            }

            if (this.assemblies.Count == 0) 
            {
                return ch;
            }

            lock (_mergeLock)
            {
                ClassHierarchyImpl chi = (ClassHierarchyImpl)ch;
                MonotonicHashSet<string> otherJars = new MonotonicHashSet<string>();
                otherJars.AddAll(chi.assemblies);
                MonotonicHashSet<string> myJars = new MonotonicHashSet<string>();
                myJars.AddAll(this.assemblies);
                if (myJars.ContainsAll(otherJars))
                {
                    return this;
                }
                if (otherJars.ContainsAll(myJars))
                {
                    return ch;
                }
                myJars.Union(otherJars);
                return new ClassHierarchyImpl(myJars.ToArray());
            }
        }

        public object Parse(INamedParameterNode np, string value)
        {
            IClassNode iface = null;
            try 
            {
                iface = (IClassNode)GetNode(np.GetFullArgName());
            } 
            catch (NameResolutionException e)
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                var ex = new IllegalStateException("Could not parse validated named parameter argument type.  NamedParameter is " + np.GetFullName() + " argument type is " + np.GetFullArgName(), e);
                Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
            Type clazz;
            string fullName;
            try 
            {
                clazz = (Type)ClassForName(iface.GetFullName());
                fullName = null;
            } 
            catch (TypeLoadException e)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Warning, LOGGER);
                clazz = null;
                fullName = iface.GetFullName();
            }

            object result = null;
            if (clazz != null) 
            {
                result = Parameterparser.Parse(clazz, value);
            }
            else
            {
                result = Parameterparser.Parse(fullName, value);                
            }

            if (result == null)
            {
                try
                {
                    INode impl = GetNode(value);
                    if (impl is IClassNode)
                    {
                        if (IsImplementation(iface, (IClassNode)impl))
                        {
                            return impl;
                        }
                    }
                    var ex =
                        new ParseException(
                            "Name<" + iface.GetFullName() + "> " + np.GetFullName() + " cannot take non-subclass " +
                            impl.GetFullName());
                    Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
                catch (NameResolutionException ec)
                {
                    Utilities.Diagnostics.Exceptions.Caught(ec, Level.Error, LOGGER);
                    var ex =
                        new ParseException(
                            "Name<" + iface.GetFullName() + "> " + np.GetFullName() + " cannot take non-class " + value,
                            ec);
                    Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
            }
            return result; 
        }

        public object ParseDefaultValue(INamedParameterNode name)
        {
            string[] vals = name.GetDefaultInstanceAsStrings();
            object[] ret = new object[vals.Length];
            for (int i = 0; i < vals.Length; i++)
            {
                string val = vals[i];
                try
                {
                    ret[i] = Parse(name, val);
                }
                catch (ParseException e)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                    var ex = new ClassHierarchyException("Could not parse default value " + val, e);
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
            }
            if (name.IsSet())
            {
                return new HashSet<object>(ret.ToList<object>());
            }
            if (name.IsList())
            {
                return new List<object>(ret.ToList<object>());
            }
            if (ret.Length == 0)
            {
                return null;
            }
            if (ret.Length == 1)
            {
                return ret[0];
            }
            var ec = new IllegalStateException("Multiple defaults for non-set named parameter! " + name.GetFullName());
            Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ec, LOGGER);
            return null; // this line would be never reached as Throw will throw an exception
        }

        public Type ClassForName(string name)
        {
            return this.GetType(name);
        }

        public Type GetType(string name)
        {
            return this.loader.GetType(name);
        }
    }
}
