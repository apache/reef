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
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Implementations.InjectionPlan
{
    public interface IInjectionFuture<out T>
    {
        T Get();
    }

    public sealed class InjectionFutureImpl<T> : IInjectionFuture<T>
    {
        private readonly InjectorImpl injector;
        private readonly Type iface; // entend from T
        private readonly T instance; 

        ////public InjectionFuture()
        ////{
        ////   injector = null;
        ////   iface = null;
        ////   instance = null;
        ////}

        public InjectionFutureImpl(IInjector injector, Type iface) 
        {
            this.injector = (InjectorImpl)injector;
            this.iface = iface;
            this.instance = default(T);
        }

        public InjectionFutureImpl(T instance)
        {
            this.injector = null;
            this.iface = null;
            this.instance = instance;
        }

        ////public bool Cancel(bool mayInterruptIfRunning) 
        ////{
        ////    return false;
        ////}

        ////public bool IsCancelled()
        ////{
        ////   return false;
        ////}

        ////public bool IsDone()
        ////{
        ////   return true;
        ////}

        public T Get() 
        {
            if (instance != null)
            {
                return instance;
            }
            lock (injector) 
            {
                T t;
                if (ReflectionUtilities.IsAssignableFromIgnoreGeneric(typeof(Name<>), iface))
                {
                    t = (T)injector.GetNamedInstance(iface);
                } 
                else 
                {
                    t = (T)injector.GetInstance(iface);
                }
                Aspect a = injector.GetAspect();
                if (a != null) 
                {
                    a.InjectionFutureInstantiated(this, t);
                }
                return t;
            }
        }
    }
}
