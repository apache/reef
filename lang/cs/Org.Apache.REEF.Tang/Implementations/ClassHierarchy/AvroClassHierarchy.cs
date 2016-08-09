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
using System.Linq;
using System.Runtime.Remoting.Messaging;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.ClassHierarchy.AvroDataContract;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Implementations.ClassHierarchy
{
    /// <summary>
    /// Deserialized Class Hierarchy 
    /// </summary>
    internal class AvroClassHierarchy : IClassHierarchy
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(AvroClassHierarchy));

        private readonly IPackageNode _rootNode;
        private readonly IDictionary<string, INode> _lookupTable = new Dictionary<string, INode>();
        private readonly IDictionary<string, IDictionary<string, string>> _aliasLookupTable = new Dictionary<string, IDictionary<string, string>>();

        /// <summary>
        /// create a AvroClassHierarchy with empty nodes and lookup table. It can be used to merge other class hierarchy to it
        /// </summary>
        internal AvroClassHierarchy()
        {
            _rootNode = new PackageNodeImpl();
        }

        /// <summary>
        /// Convert AvroNode into AvroClassHierarchy object
        /// </summary>
        /// <param name="root"></param>
        internal AvroClassHierarchy(AvroNode root)
        {
            _rootNode = new PackageNodeImpl();
            if (root.packageNode == null)
            {
                Utilities.Diagnostics.Exceptions.Throw(new ArgumentException("Expected a package node.  Got: " + root), LOGGER); 
            }

            foreach (AvroNode child in root.children)
            {
                ParseSubHierarchy(_rootNode, child);
            }
            
            BuildHashTable(_rootNode);

            foreach (AvroNode child in root.children)
            {
                WireUpInheritanceRelationships(child);
            }
        }

        private void ParseSubHierarchy(INode parent, AvroNode n)
        {
            INode parsed = null;
            if (n.packageNode != null && !n.packageNode.Equals(string.Empty))
            {
                parsed = new PackageNodeImpl(parent, n.name, n.fullName);
            }
            else if (n.namedParameterNode != null && !n.namedParameterNode.Equals(string.Empty))
            {
                AvroNamedParameterNode np = (AvroNamedParameterNode)n.namedParameterNode;
                parsed = new NamedParameterNodeImpl(parent, n.name,
                    n.fullName, np.fullArgClassName, np.simpleArgClassName,
                    np.isSet, np.isList, np.documentation, np.shortName,
                    np.instanceDefault.ToArray());
            }
            else if (n.classNode != null && !n.classNode.Equals(string.Empty))
            {
                AvroClassNode cn = (AvroClassNode)n.classNode;
                IList<IConstructorDef> injectableConstructors = new List<IConstructorDef>();
                IList<IConstructorDef> allConstructors = new List<IConstructorDef>();

                foreach (AvroConstructorDef injectable in cn.injectableConstructors)
                {
                    IConstructorDef def = ParseConstructorDef(injectable, true);
                    injectableConstructors.Add(def);
                    allConstructors.Add(def);
                }
                foreach (AvroConstructorDef other in cn.otherConstructors)
                {
                    IConstructorDef def = ParseConstructorDef(other, false);
                    allConstructors.Add(def);
                }

                parsed = new ClassNodeImpl(parent, n.name, n.fullName,
                cn.isUnit, cn.isInjectionCandidate,
                cn.isExternalConstructor, injectableConstructors,
                allConstructors, cn.defaultImplementation);
            }
            else
            {
                Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException("Bad protocol buffer: got abstract node" + n), LOGGER);
            }

            foreach (AvroNode child in n.children)
            {
                ParseSubHierarchy(parsed, child);
            }
        }

        private void WireUpInheritanceRelationships(AvroNode n)
        {
            if (n.classNode != null && !n.classNode.Equals(string.Empty))
            {
                AvroClassNode cn = (AvroClassNode)n.classNode;
                IClassNode iface = null;

                try
                {
                    iface = (IClassNode)GetNode(n.fullName);
                }
                catch (NameResolutionException e)
                {
                    Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                    var ex = new IllegalStateException("When reading protocol buffer node "
                        + n.fullName + " does not exist.  Full record is " + n, e);
                    Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }

                foreach (string impl in cn.implFullNames)
                {
                    try
                    {
                        iface.PutImpl((IClassNode)GetNode(impl));
                    }
                    catch (NameResolutionException e)
                    {
                        Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                        var ex = new IllegalStateException("When reading protocol buffer node "
                            + n + " refers to non-existent implementation:" + impl);
                        Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                    }
                    catch (InvalidCastException e)
                    {
                        Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                        try
                        {
                            var ex = new IllegalStateException(
                                "When reading protocol buffer node " + n
                                + " found implementation" + GetNode(impl)
                                + " which is not a ClassNode!");
                            Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                        }
                        catch (NameResolutionException ne)
                        {
                            Utilities.Diagnostics.Exceptions.Caught(ne, Level.Error, LOGGER);
                            var ex = new IllegalStateException(
                                "Got 'cant happen' exception when producing error message for " + e);
                            Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Build hash table to index the node
        /// </summary>
        /// <param name="n"></param>
        public void BuildHashTable(INode n)
        {
            foreach (INode child in n.GetChildren())
            {
                _lookupTable.Add(child.GetFullName(), child);
                if (child is INamedParameterNode)
                {
                    AddAlias((INamedParameterNode)child);
                }
                BuildHashTable(child);
            }
        }

        private void AddAlias(INamedParameterNode np)
        {
            if (np.GetAlias() != null && !np.GetAlias().Equals(string.Empty))
            {
                IDictionary<string, string> mapping = null;
                _aliasLookupTable.TryGetValue(np.GetAliasLanguage().ToString(), out mapping);
                if (mapping == null)
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
                    var e = new TangApplicationException(string.Format(CultureInfo.CurrentCulture, "Duplicated alias {0} on named parameter {1}.", np.GetAlias(), np.GetFullName()));
                    Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
            }
        }

        /// <summary>
        /// Get a Node from the class hierarchy 
        /// </summary>
        /// <param name="fullName"></param>
        /// <returns></returns>
        public INode GetNode(string fullName)
        {
            INode ret;
            _lookupTable.TryGetValue(fullName, out ret);
            if (ret == null)
            {
                var ex = new NameResolutionException(fullName, "Cannot resolve the name from the class hierarchy during deserialization: " + fullName);
                Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
            return ret;
        }

        /// <summary>
        /// This method get INode from deSerialized class hierarchy by fullName. 
        /// If the name is not found, it will found alias for aliasLanguage. If alias is found,
        /// it will use the alias to do look up again. 
        /// </summary>
        /// <param name="fullName"></param>
        /// <param name="aliasLanguage"></param>
        /// <returns></returns>
        public INode GetNode(string fullName, string aliasLanguage)
        {
            INode ret = null;
            _lookupTable.TryGetValue(fullName, out ret);
            if (ret == null)
            {
                IDictionary<string, string> mapping = null;
                string assemblyName = null;
                _aliasLookupTable.TryGetValue(aliasLanguage, out mapping);
                if (mapping != null)
                {
                    mapping.TryGetValue(fullName, out assemblyName);
                    if (assemblyName != null)
                    {
                        _lookupTable.TryGetValue(assemblyName, out ret);
                    }
                }
                if (mapping == null || assemblyName == null || ret == null)
                {
                    var ex = new NameResolutionException(fullName, "Cannot resolve the name from the class hierarchy during de-serialization: " + fullName);
                    Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
            }
            return ret;
        }

        /// <summary>
        /// return root node
        /// </summary>
        /// <returns></returns>
        public INode GetNamespace()
        {
            return _rootNode;
        }

        /// <summary>
        /// Check if impl is an implementation of inter
        /// </summary>
        /// <param name="inter"></param>
        /// <param name="impl"></param>
        /// <returns></returns>
        public bool IsImplementation(IClassNode inter, IClassNode impl)
        {
            return impl.IsImplementationOf(inter);
        }

        /// <summary>
        /// Merge two Class Hierarchy 
        /// </summary>
        /// <param name="ch"></param>
        /// <returns></returns>
        public IClassHierarchy Merge(IClassHierarchy ch)
        {
            if (this == ch)
            {
                return this;
            }

            if (!(ch is AvroClassHierarchy))
            {
                Utilities.Diagnostics.Exceptions.Throw(new NotSupportedException(
                                                            "Cannot merge ExternalClassHierarchies yet!"), LOGGER);
            }

            AvroClassHierarchy ach = (AvroClassHierarchy)ch;
            foreach (var pair in ach._lookupTable)
            {
                if (!this._lookupTable.ContainsKey(pair.Key))
                {
                    this._lookupTable.Add(pair);
                }
            }

            foreach (INode n in ch.GetNamespace().GetChildren())
            {
                if (!_rootNode.Contains(n.GetFullName()))
                {
                    if (n is INamedParameterNode)
                    {
                        INamedParameterNode np = (INamedParameterNode)n;
                        new NamedParameterNodeImpl(this._rootNode, np.GetName(),
                                                   np.GetFullName(), np.GetFullArgName(), np.GetSimpleArgName(),
                                                   np.IsSet(), np.IsList(), np.GetDocumentation(), np.GetShortName(),
                                                   np.GetDefaultInstanceAsStrings().ToArray());
                    }
                    else if (n is IClassNode)
                    {
                        IClassNode cn = (IClassNode)n;
                        new ClassNodeImpl(_rootNode, cn.GetName(), cn.GetFullName(),
                                          cn.IsUnit(), cn.IsInjectionCandidate(),
                                          cn.IsExternalConstructor(), cn.GetInjectableConstructors(),
                                          cn.GetAllConstructors(), cn.GetDefaultImplementation());
                    }
                }
            }
            return this;
        }

        private IConstructorDef ParseConstructorDef(AvroConstructorDef def, bool isInjectable)
        {
            IList<IConstructorArg> args = new List<IConstructorArg>();
            foreach (AvroConstructorArg arg in def.constructorArgs)
            {
                args.Add(new ConstructorArgImpl(arg.fullArgClassName, arg.namedParameterName, arg.isInjectionFuture));
            }
            return new ConstructorDefImpl(def.fullClassName, args.ToArray(), isInjectable);
        }
    }
}
