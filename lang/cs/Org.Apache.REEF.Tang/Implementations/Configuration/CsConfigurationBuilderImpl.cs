/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Implementations.Configuration
{
    public class CsConfigurationBuilderImpl : ConfigurationBuilderImpl, ICsInternalConfigurationBuilder
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(CsConfigurationBuilderImpl));

        #region Constructors
        public CsConfigurationBuilderImpl(string[] assemblies, IConfiguration[] confs, Type[] parsers)
            : base(assemblies, confs, parsers)
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
        #endregion Constructors

        #region ICsConfigurationBuilder
        /// <summary>
        /// Builds this instance.
        /// </summary>
        /// <returns></returns>
        public CsConfigurationImpl build()
        {
            return new CsConfigurationImpl(new CsConfigurationBuilderImpl(this));
        }

        /// <summary>
        /// Binds a string to a named parameter.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        /// <exception cref="BindException">Detected type mismatch when setting named parameter  + name
        /// + Expected NamedParameterNode, but namespace contains a  + np</exception>
        public ICsConfigurationBuilder BindNamedParameter(Type name, string value)
        {
            if (value == null)
            {
                var ex = new IllegalStateException(string.Format(CultureInfo.CurrentCulture, "The value null set to the named parameter {0} is illegal.", name));
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
            INode np = GetNode(name);
            if (np is INamedParameterNode)
            {
                BindParameter((INamedParameterNode)np, value);
            }
            else
            {
                var ex = new BindException(
                    "Detected type mismatch when setting named parameter " + name
                    + "  Expected NamedParameterNode, but namespace contains a " + np);
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
            return this;
        }

        /// <summary>
        /// Binds the class impl as the implementation of the interface iface
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        /// <returns></returns>
        public ICsConfigurationBuilder BindImplementation(Type iface, Type impl)
        {
            INode cn = GetNode(iface);
            INode dn = GetNode(impl);
            if (!(cn is IClassNode))
            {
                var ex = new BindException(
                    "bindImplementation passed interface that resolved to " + cn
                    + " expected a ClassNode<?>");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
            if (!(dn is IClassNode))
            {
                var ex = new BindException(
                    "bindImplementation passed implementation that resolved to " + dn
                    + " expected a ClassNode<?>");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
            BindImplementation((IClassNode)cn, (IClassNode)dn);
            return this;
        }

        /// <summary>
        /// Binds the implementation to the interface
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        /// <returns></returns>
        public ICsConfigurationBuilder BindImplementation<U, T>(GenericType<U> iface, GenericType<T> impl)
            where T : U
        {
            return BindImplementation(typeof(U), typeof(T));
        }

        /// <summary>
        /// Binds a value to a named parameter.
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="name">The name.</param>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        public ICsConfigurationBuilder BindNamedParameter<U, T>(GenericType<U> name, string value)
            where U : Name<T>
        {
            return BindNamedParameter(typeof(U), value);
        }

        /// <summary>
        /// Binds an implementation to a named parameter.
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        /// <returns></returns>
        public ICsConfigurationBuilder BindNamedParameter<U, V, T>(GenericType<U> iface, GenericType<V> impl)
            where U : Name<T>
            where V : T
        {
            return ((ICsInternalConfigurationBuilder)this).BindNamedParameter(typeof(U), typeof(V));
        }

        /// <summary>
        /// Binds an external constructor.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="U"></typeparam>
        /// <param name="c">The c.</param>
        /// <param name="v">The v.</param>
        /// <returns></returns>
        public ICsConfigurationBuilder BindConstructor<T, U>(GenericType<T> c, GenericType<U> v)
            where U : IExternalConstructor<T>
        {
            return ((ICsInternalConfigurationBuilder)this).BindConstructor(typeof(T), typeof(U));
        }

        //// public <T> void bindSetEntry(Class<? extends Name<Set<T>>> iface, String value) throws BindException;

        /// <summary>
        /// Binds a string value to a named parameter of ISet.
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="iface">The iface.</param>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        public ICsConfigurationBuilder BindSetEntry<U, T>(GenericType<U> iface, string value)
            where U : Name<ISet<T>>
        {
            return ((ICsInternalConfigurationBuilder)this).BindSetEntry(typeof(U), value);
        }

        //// public <T> void bindSetEntry(Class<? extends Name<Set<T>>> iface, Class<? extends T> impl) throws BindException;

        /// <summary>
        /// Binds an implementation of T to a named parameter of ISet of T.
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        /// <returns></returns>
        public ICsConfigurationBuilder BindSetEntry<U, V, T>(GenericType<U> iface, GenericType<V> impl)
            where U : Name<ISet<T>>
            where V : T
        {
            return ((ICsInternalConfigurationBuilder)this).BindSetEntry(typeof(U), typeof(V));
        }

        public ICsConfigurationBuilder BindList<U, V, T>(GenericType<U> iface, IList<GenericType<V>> impl)
            where U : Name<IList<T>>
            where V : T
        {
            IList<Type> implTypes = new List<Type>();
            foreach (var item in impl)
            {
                implTypes.Add(item.TypeT);
            }
            return ((ICsInternalConfigurationBuilder)this).BindList(typeof(U), implTypes); 
        }

        public ICsConfigurationBuilder BindList<U, T>(GenericType<U> iface, IList<string> impl)
            where U : Name<IList<T>>
        {
            return ((ICsInternalConfigurationBuilder)this).BindList(typeof(U), impl); 
        }

        public ICsConfigurationBuilder BindList(Type iface, IList<Type> implList)
        {
            INode n = GetNode(iface);
            IList<INode> result = new List<INode>();

            if (!(n is INamedParameterNode))
            {
                var ex = new BindException("BindList got an interface that resolved to " + n + "; expected a NamedParameter");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }

            Type listType = ReflectionUtilities.GetInterfaceTarget(typeof(Name<>), iface);
            if (!ReflectionUtilities.IsGenericTypeof(typeof(IList<>), listType))
            {
                var ex = new BindException("BindList got a NamedParameter that takes a " + listType + "; expected List<...>");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }

            if (implList != null && implList.Count > 0)
            {
                Type valType = ReflectionUtilities.GetInterfaceTarget(typeof(IList<>), listType);

                foreach (var item in implList)
                {
                    if (!valType.IsAssignableFrom((Type)item))
                    {
                        var ex =
                            new BindException("BindList got implementation " + item +
                                                " that is incompatible with expected type " + valType);
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                    }
                    result.Add(GetNode(item));
                }
            }

            BindList((INamedParameterNode)n, result);
            return this;
        }
        #endregion ICsConfigurationBuilder

        #region ICsInternalConfigurationBuilder
        /// <summary>
        /// Bind named parameters, implementations or external constructors, depending
        /// on the types of the classes passed in.
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        /// <returns></returns>
        ICsInternalConfigurationBuilder ICsInternalConfigurationBuilder.Bind(Type iface, Type impl)
        {
            Bind(GetNode(iface), GetNode(impl));
            return this;
        }

        /// <summary>
        /// Binds an implementation for a named parameter.
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        /// <returns></returns>
        /// <exception cref="BindException">Type mismatch when setting named parameter  + ifaceN
        /// +  Expected NamedParameterNode</exception>
        ICsInternalConfigurationBuilder ICsInternalConfigurationBuilder.BindNamedParameter(Type iface, Type impl)
        {
            INode ifaceN = GetNode(iface);
            INode implN = GetNode(impl);
            if (!(ifaceN is INamedParameterNode))
            {
                var ex = new BindException("Type mismatch when setting named parameter " + ifaceN
                    + " Expected NamedParameterNode");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
            Bind(ifaceN, implN);
            return this;
        }

        //// public <T> void bindSetEntry(Class<? extends Name<Set<T>>> iface, String value) throws BindException;

        /// <summary>
        /// Binds a string value to to a named parameter of ISet entry
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        /// <exception cref="BindException">BindSetEntry got an interface that resolved to  + n + ; expected a NamedParameter</exception>
        ICsInternalConfigurationBuilder ICsInternalConfigurationBuilder.BindSetEntry(Type iface, string value)
        {
            INode n = GetNode(iface);

            if (!(n is INamedParameterNode))
            {
                var ex = new BindException("BindSetEntry got an interface that resolved to " + n + "; expected a NamedParameter");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
            Type setType = ReflectionUtilities.GetInterfaceTarget(typeof(Name<>), iface);

            // check if setType is ISet
            if (ReflectionUtilities.GetInterfaceTarget(typeof(ISet<>), setType) == null)
            {
                var ex = new BindException("BindSetEntry got a NamedParameter that takes a " + setType + "; expected Set<...>");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
            //// Type valType = ReflectionUtilities.getInterfaceTarget(Set.class, setType);
            BindSetEntry((INamedParameterNode)n, value);
            return this;
        }

        //// public <T> void bindSetEntry(Class<? extends Name<Set<T>>> iface, Class<? extends T> impl) throws BindException;

        /// <summary>
        /// Binds an implementation to a named parameter of ISet entry.
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        /// <returns></returns>
        /// <exception cref="BindException">BindSetEntry got an interface that resolved to  + n + ; expected a NamedParameter</exception>
        ICsInternalConfigurationBuilder ICsInternalConfigurationBuilder.BindSetEntry(Type iface, Type impl)
        {
            INode n = GetNode(iface);
            INode m = GetNode(impl);

            if (!(n is INamedParameterNode))
            {
                var ex = new BindException("BindSetEntry got an interface that resolved to " + n + "; expected a NamedParameter");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
            Type setType = ReflectionUtilities.GetInterfaceTarget(typeof(Name<>), iface);

            // if (!ReflectionUtilities.GetRawClass(setType).Equals(typeof(ISet<>)))
            if (!ReflectionUtilities.IsGenericTypeof(typeof(ISet<>), setType))
            {
                var ex = new BindException("BindSetEntry got a NamedParameter that takes a " + setType + "; expected Set<...>");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }

            Type valType = ReflectionUtilities.GetInterfaceTarget(typeof(ISet<>), setType);

            //// if (!ReflectionUtilities.GetRawClass(valType).IsAssignableFrom(impl))
            if (!valType.IsAssignableFrom(impl))
            {
                var ex = new BindException("BindSetEntry got implementation " + impl + " that is incompatible with expected type " + valType);
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }

            BindSetEntry((INamedParameterNode)n, m);
            return this;
        }

        ICsInternalConfigurationBuilder ICsInternalConfigurationBuilder.BindList(Type iface, IList<string> implList)
        {
            INode n = GetNode(iface);

            if (!(n is INamedParameterNode))
            {
                var ex = new BindException("BindList got an interface that resolved to " + n + "; expected a NamedParameter");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }

            Type listType = ReflectionUtilities.GetInterfaceTarget(typeof(Name<>), iface);
            if (!ReflectionUtilities.IsGenericTypeof(typeof(IList<>), listType))
            {
                var ex = new BindException("BindList got a NamedParameter that takes a " + listType + "; expected List<...>");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }

            BindList((INamedParameterNode)n, implList);
            return this;
        }

        //// public <T> void bindConstructor(Class<T> c, Class<? extends ExternalConstructor<? extends T>> v) throws BindException;

        /// <summary>
        /// Binds an external constructor.
        /// </summary>
        /// <param name="c">The c.</param>
        /// <param name="v">The v.</param>
        /// <returns></returns>
        /// <exception cref="BindException">BindConstructor got class that resolved to  + n + ; expected ClassNode</exception>
        ICsInternalConfigurationBuilder ICsInternalConfigurationBuilder.BindConstructor(Type c, Type v)
        {
            INode n = GetNode(c);
            INode m = GetNode(v);

            if (!(n is IClassNode))
            {
                var ex = new BindException("BindConstructor got class that resolved to " + n + "; expected ClassNode");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
            if (!(m is IClassNode))
            {
                var ex = new BindException("BindConstructor got class that resolved to " + m + "; expected ClassNode");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
            BindConstructor((IClassNode)n, (IClassNode)m);
            return this;
        }
        #endregion ICsInternalConfigurationBuilder

        #region extension methods
        
        public ICsConfigurationBuilder BindNamedParam<TName, TType>(string str) where TName : Name<TType>
        {
            return BindNamedParameter<TName, TType>(GenericType<TName>.Class, str);
        }

        public ICsConfigurationBuilder BindStringNamedParam<T>(string str) where T : Name<string>
        {
            return BindNamedParameter<T, string>(GenericType<T>.Class, str);
        }

        public ICsConfigurationBuilder BindIntNamedParam<T>(string str) where T : Name<int>
        {
            return BindNamedParameter<T, int>(GenericType<T>.Class, str);
        }

        public ICsConfigurationBuilder BindNamedParameter<U, V, T>()
            where U : Name<T>
            where V : T
        {
            return BindNamedParameter<U, V, T>(GenericType<U>.Class, GenericType<V>.Class);
        }

        public ICsConfigurationBuilder BindSetEntry<T1, T2, T3>()
            where T1 : Name<ISet<T3>>
            where T2 : T3
        {
            return BindSetEntry<T1, T2, T3>(GenericType<T1>.Class, GenericType<T2>.Class);
        }

        public ICsConfigurationBuilder BindSetEntry<U, T>(string value) where U : Name<ISet<T>>
        {
            return BindSetEntry<U, T>(GenericType<U>.Class, value);
        }

        public ICsConfigurationBuilder BindImplementation<T1, T2>() where T2 : T1
        {
            return BindImplementation(GenericType<T1>.Class, GenericType<T2>.Class);
        }

        public ICsConfigurationBuilder BindList<U, T>(IList<string> impl) where U : Name<IList<T>>
        {
            return BindList<U, T>(GenericType<U>.Class, impl);
        }

        public ICsConfigurationBuilder BindConstructor<T, U>() where U : IExternalConstructor<T>
        {
            return BindConstructor<T, U>(GenericType<T>.Class, GenericType<U>.Class);
        }

        #endregion extension methods

        private INode GetNode(Type c)
        {
            return ((ICsClassHierarchy)ClassHierarchy).GetNode(c);
        }
    }
}