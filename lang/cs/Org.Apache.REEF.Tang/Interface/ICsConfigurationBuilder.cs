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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Interface
{
    public interface ICsConfigurationBuilder : IConfigurationBuilder
    {
        /// <summary>
        /// Binds the named parameter.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="value">The value.</param>
        ICsConfigurationBuilder BindNamedParameter(Type name, string value);  // name must extend from Name<T>

        /// <summary>
        /// Binds the class impl as the implementation of the interface iface
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        ICsConfigurationBuilder BindImplementation(Type iface, Type impl);

        /// <summary>
        /// Binds the List entry.
        /// </summary>
        /// <param name="iface">The iface. It is a Name of IList</param>
        /// <param name="impl">The impl.</param>
        ICsConfigurationBuilder BindList(Type iface, IList<Type> impl);

        /// <summary>
        /// Binds the named parameter.
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="name">The name.</param>
        /// <param name="value">The value.</param>
        ICsConfigurationBuilder BindNamedParameter<U, T>(GenericType<U> name, string value)
            where U : Name<T>;

        /// <summary>
        /// Binds the named parameter.
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        ICsConfigurationBuilder BindNamedParameter<U, V, T>(GenericType<U> iface, GenericType<V> impl)
            where U : Name<T>
            where V : T;

        /// <summary>
        /// Binds the implementation.
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        ICsConfigurationBuilder BindImplementation<U, T>(GenericType<U> iface, GenericType<T> impl)
            where T : U;

        // public <T> void bindConstructor(Class<T> c, Class<? extends ExternalConstructor<? extends T>> v) throws BindException;
        ICsConfigurationBuilder BindConstructor<T, U>(GenericType<T> c, GenericType<U> v)
            where U : IExternalConstructor<T>;
  
        // public <T> void bindSetEntry(Class<? extends Name<Set<T>>> iface, String value) throws BindException;
        ICsConfigurationBuilder BindSetEntry<U, T>(GenericType<U> iface, string value)
            where U : Name<ISet<T>>;

         // public <T> void bindSetEntry(Class<? extends Name<Set<T>>> iface, Class<? extends T> impl) throws BindException;
        ICsConfigurationBuilder BindSetEntry<U, V, T>(GenericType<U> iface, GenericType<V> impl)
            where U : Name<ISet<T>>
            where V : T;

        ICsConfigurationBuilder BindList<U, V, T>(GenericType<U> iface, IList<GenericType<V>> impl)
            where U : Name<IList<T>>
            where V : T;

        ICsConfigurationBuilder BindList<U, T>(GenericType<U> iface, IList<string> impl)
            where U : Name<IList<T>>;

        ICsConfigurationBuilder BindNamedParameter<U, V, T>()
            where U : Name<T>
            where V : T;

        ICsConfigurationBuilder BindNamedParam<TName, TType>(string str) where TName : Name<TType>;

        ICsConfigurationBuilder BindStringNamedParam<T>(string str) where T : Name<string>;

        ICsConfigurationBuilder BindIntNamedParam<T>(string str) where T : Name<int>;

        ICsConfigurationBuilder BindImplementation<T1, T2>() where T2 : T1;

        ICsConfigurationBuilder BindSetEntry<T1, T2, T3>() where T1 : Name<ISet<T3>> where T2 : T3;

        ICsConfigurationBuilder BindSetEntry<U, T>(string value) where U : Name<ISet<T>>;

        ICsConfigurationBuilder BindList<U, T>(IList<string> impl) where U : Name<IList<T>>;

        ICsConfigurationBuilder BindConstructor<T, U>() where U : IExternalConstructor<T>;
    }
}
