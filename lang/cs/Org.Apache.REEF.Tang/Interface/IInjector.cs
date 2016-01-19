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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.InjectionPlan;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Interface
{
    public interface IInjector
    {
        /// <summary>
        /// Gets an instance of iface, or the implementation that has been bound to it.
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <returns></returns>
        object GetInstance(Type iface);

        /// <summary>
        /// Gets the instance for a given class
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        T GetInstance<T>() where T : class;

        /// <summary>
        /// Gets the instance for a given class name
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <returns></returns>
        object GetInstance(string iface);

        /// <summary>
        /// Get an instance of the named parameter. 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="U"></typeparam>
        /// <param name="clazz">The clazz.</param>
        /// <returns></returns>
        U GetNamedInstance<T, U>(GenericType<T> clazz) where T : Name<U>;

        /// <summary>
        /// Get an instance of the named parameter. 
        /// </summary>
        /// <param name="t">t, it must inherit from Name</param>
        /// <returns></returns>
        object GetNamedInstance(Type t);

        /// <summary>
        /// Gets the injection plan.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns></returns>
        InjectionPlan GetInjectionPlan(Type name);

        /// <summary>
        /// Gets the injection plan for a given class name
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns></returns>
        InjectionPlan GetInjectionPlan(string name);

        /// <summary>
        /// Binds the aspect.
        /// </summary>
        /// <param name="a">A.</param>
        void BindAspect(Aspect a);

        /// <summary>
        /// Gets the aspect.
        /// </summary>
        /// <returns></returns>
        Aspect GetAspect();

        /// <summary>
        /// Determines whether the specified name is injectable.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>
        ///   <c>true</c> if the specified name is injectable; otherwise, <c>false</c>.
        /// </returns>
        bool IsInjectable(string name);

        /// <summary>
        /// Determines whether [is parameter set] [the specified name].
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>
        ///   <c>true</c> if [is parameter set] [the specified name]; otherwise, <c>false</c>.
        /// </returns>
        bool IsParameterSet(string name);

        /// <summary>
        /// Determines whether the specified clazz is injectable.
        /// </summary>
        /// <param name="clazz">The clazz.</param>
        /// <returns>
        ///   <c>true</c> if the specified clazz is injectable; otherwise, <c>false</c>.
        /// </returns>
        bool IsInjectable(Type clazz);

        /// <summary>
        /// Determines whether [is parameter set] [the specified name].
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>
        ///   <c>true</c> if [is parameter set] [the specified name]; otherwise, <c>false</c>.
        /// </returns>
        bool IsParameterSet(Type name);

        /// <summary>
        /// Forks the injector.
        /// </summary>
        /// <returns></returns>
        IInjector ForkInjector();

        /// <summary>
        /// Forks the injector.
        /// </summary>
        /// <param name="configurations">The configurations.</param>
        /// <returns></returns>
        IInjector ForkInjector(params IConfiguration[] configurations);

        /// <summary>
        /// Binds the volatile instance.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="iface">The iface.</param>
        /// <param name="inst">The inst.</param>
        void BindVolatileInstance<T>(GenericType<T> iface, T inst);

        /// <summary>
        /// Binds the volatile parameter.
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="iface">The iface.</param>
        /// <param name="inst">The inst.</param>
        void BindVolatileParameter<U, T>(GenericType<U> iface, T inst) where U : Name<T>;

        /// <summary>
        /// Gets the named instance.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="U"></typeparam>
        /// <returns></returns>
        U GetNamedInstance<T, U>() where T : Name<U>;

        /// <summary>
        /// Binds the volatile instance.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="inst">The inst.</param>
        void BindVolatileInstance<T>(T inst);

        /// <summary>
        /// Binds the volatile parameter.
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="inst">The inst.</param>
        void BindVolatileParameter<U, T>(T inst) where U : Name<T>;
      }
}
