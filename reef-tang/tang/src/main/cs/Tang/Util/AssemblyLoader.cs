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
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Util
{
    public class AssemblyLoader
    {
        public IList<Assembly> Assemblies { get; set; }

        public AssemblyLoader(string[] files)
        {
            Assemblies = new List<Assembly>();
            foreach (var a in files)
            {
                Assemblies.Add(Assembly.Load(a));
            }
        }

        public Type GetType(string name)
        {
            Type t = Type.GetType(name);
            if (t == null)
            {
                foreach (var a in Assemblies)
                {
                    t = a.GetType(name);
                    if (t != null)
                    {
                        return t;
                    }
                }
            }
            return t;
        }
    }
}
