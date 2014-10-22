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
using Com.Microsoft.Tang.Exceptions;
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.Implementations
{
    public class InjectionFuturePlan : InjectionPlan
    {
        public InjectionFuturePlan(INode name) : base (name)
        {
        }

        public override int GetNumAlternatives()
        {
            return 1;
        }

        public override bool IsAmbiguous()
        {
            return false;
        }

        public override bool IsInjectable()
        {
            return true;
        }

        public override bool HasFutureDependency()
        {
            return true;
        }

        public override string ToAmbiguousInjectString()
        {
            throw new UnsupportedOperationException("InjectionFuturePlan cannot be ambiguous!");
        }

        public override string ToInfeasibleInjectString()
        {
            throw new UnsupportedOperationException("InjectionFuturePlan is always feasible!");
        }

        public override bool IsInfeasibleLeaf()
        {
            return false;
        }

        public override string ToShallowString()
        {
            return "InjectionFuture<"+GetNode().GetFullName()+">";
        }
    }
}
