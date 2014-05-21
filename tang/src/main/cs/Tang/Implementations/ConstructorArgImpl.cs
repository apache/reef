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

namespace Com.Microsoft.Tang.Implementations
{
    public class ConstructorArgImpl : IConstructorArg
    {
        private readonly String type;
        private readonly String name;
        private readonly bool isInjectionFuture;

        public ConstructorArgImpl(String type, String namedParameterName, bool isInjectionFuture)
        {
            this.type = type;
            this.name = namedParameterName;
            this.isInjectionFuture = isInjectionFuture;
        }

        public string GetName()
        {
            return name == null ? type : name;
        }

        public string GetNamedParameterName()
        {
            return name;
        }

        public string Gettype()
        {
            return type;
        }

        public bool IsInjectionFuture()
        {
            return isInjectionFuture;
        }

        public override String ToString()
        {
            return name == null ? type : type + " " + name;
        }

        public override bool Equals(Object o)
        {
            ConstructorArgImpl arg = (ConstructorArgImpl)o;
            if (!type.Equals(arg.type))
            {
                return false;
            }
            if (name == null && arg.name == null)
            {
                return true;
            }
            if (name == null && arg.name != null)
            {
                return false;
            }
            if (name != null && arg.name == null)
            {
                return false;
            }
            return name.Equals(arg.name);

        }
    }
}
