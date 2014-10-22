/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Implementations;

namespace Com.Microsoft.Tang.Interface
{
    public interface IInjector
    {
        /// <summary>
        /// Gets an instance of iface, or the implementation that has been bound to it.
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <returns></returns>
        object GetInstance(Type iface);

        object GetInstance(string iface);

        object GetNamedInstance(Type iface); //iface must implement Name<T>
        
        InjectionPlan GetInjectionPlan(Type name);

        void BindAspect(Aspect a);

        Aspect GetAspect();

        bool IsInjectable(string name);

        bool IsParameterSet(String name);

        bool IsInjectable(Type clazz);

        bool isParameterSet(Type name);

        IInjector ForkInjector();

        IInjector ForkInjector(IConfiguration[] configurations);
    }
}
