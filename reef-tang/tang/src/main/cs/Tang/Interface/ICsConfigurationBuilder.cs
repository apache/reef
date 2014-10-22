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

namespace Com.Microsoft.Tang.Interface
{
    public interface ICsConfigurationBuilder : IConfigurationBuilder
    {
        /// <summary>
        /// Bind named parameters, implementations or external constructors, depending
        /// on the types of the classes passed in.
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        void Bind(Type iface, Type impl);

        /// <summary>
        /// Binds the class impl as the implementation of the interface iface
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        void BindImplementation(Type iface, Type impl);

        /// <summary>
        /// Binds a value to the named parameter.
        /// </summary>
        /// <param name="name">A Type that is extended from Name<T></param>
        /// <param name="value">A string representing the value of the parameter.</param>
        void BindNamedParameter(Type name, string value);  //name must extend from Name<T>


        /// <summary>
        /// Binds the named parameter. iface must extend from Name<T> and impl extend from T
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        void BindNamedParameter(Type iface, Type impl); 
    }
}
