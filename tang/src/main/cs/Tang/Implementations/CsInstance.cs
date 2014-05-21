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
    public class CsInstance : InjectionPlan
    {
        public readonly object instance;

        public CsInstance(INode name, object instance) : base(name)
        {
            this.instance = instance;
        }
        public override int GetNumAlternatives()
        {
            return instance == null ? 0 : 1;
        }

        public override string ToString()
        {
            return GetNode() + " = " + instance;
        }

        public override bool IsAmbiguous()
        {
            return false;
        }

        public override bool IsInjectable()
        {
            return instance != null;
        }

        public string GetInstanceAsString()
        {
            return instance.ToString();
        }

        public override bool HasFutureDependency()
        {
            return false;
        }

        public override string ToAmbiguousInjectString()
        {
            throw new ArgumentException("toAmbiguousInjectString called on JavaInstance!" + this.ToString());
        }

        public override string ToInfeasibleInjectString()
        {
            return GetNode() + " is not bound.";
        }

        public override bool IsInfeasibleLeaf()
        {
            return true;
        }

        public override string ToShallowString()
        {
            return ToString();
        }
    }
}
