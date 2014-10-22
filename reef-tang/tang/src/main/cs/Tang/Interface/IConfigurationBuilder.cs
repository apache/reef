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
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.Interface
{
    public interface IConfigurationBuilder
    {
        void AddConfiguration(IConfiguration c); 
        IClassHierarchy GetClassHierarchy();
        IConfiguration Build();
        void Bind(string iface, string impl);
        void Bind(INode key, INode value);

        void BindConstructor(IClassNode k, IClassNode v); //v extended from ExternalConstructor
        string ClassPrettyDefaultString(string longName);
        string ClassPrettyDescriptionString(string longName);


    }
}
