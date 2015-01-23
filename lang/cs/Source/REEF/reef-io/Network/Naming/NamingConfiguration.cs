﻿/**
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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.Reef.Tang.Formats;
using Org.Apache.Reef.Tang.Util;

namespace Org.Apache.Reef.Naming
{
    public class NamingConfiguration : ConfigurationModuleBuilder
    {
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly RequiredParameter<string> NameServerAddress = new RequiredParameter<string>();

        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly RequiredParameter<int> NameServerPort = new RequiredParameter<int>();

        public static ConfigurationModule ConfigurationModule
        {
            get
            {
                return new NamingConfiguration()
                    .BindNamedParameter(GenericType<NamingConfigurationOptions.NameServerAddress>.Class, NameServerAddress)
                    .BindNamedParameter(GenericType<NamingConfigurationOptions.NameServerPort>.Class, NameServerPort)
                    .Build();
            }
        }
    }
}
