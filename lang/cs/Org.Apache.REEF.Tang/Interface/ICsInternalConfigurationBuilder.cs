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

namespace Org.Apache.REEF.Tang.Interface
{
    internal interface ICsInternalConfigurationBuilder : ICsConfigurationBuilder
    {
        /// <summary>
        /// Bind named parameters, implementations or external constructors, depending
        /// on the types of the classes passed in.
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        ICsInternalConfigurationBuilder Bind(Type iface, Type impl);

        /// <summary>
        /// Binds the named parameter.
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        ICsInternalConfigurationBuilder BindNamedParameter(Type iface, Type impl);

        /// <summary>
        /// Binds an external constructor.
        /// </summary>
        /// <param name="c">The c.</param>
        /// <param name="v">The v.</param>
        ICsInternalConfigurationBuilder BindConstructor(Type c, Type v);

        /// <summary>
        /// Binds the set entry.
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="value">The value.</param>
        ICsInternalConfigurationBuilder BindSetEntry(Type iface, string value);

        /// <summary>
        /// Binds the set entry.
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        ICsInternalConfigurationBuilder BindSetEntry(Type iface, Type impl);

        /// <summary>
        /// Binds the List entry.
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        ICsInternalConfigurationBuilder BindList(Type iface, IList<string> impl);
    }
}