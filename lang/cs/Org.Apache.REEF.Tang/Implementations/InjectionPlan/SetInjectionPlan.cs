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

using System.Collections.Generic;
using System.Text;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Implementations.InjectionPlan
{
    public class SetInjectionPlan : InjectionPlan
    {
        private readonly ISet<InjectionPlan> entries = new MonotonicHashSet<InjectionPlan>();
        private readonly int numAlternatives;
        private readonly bool isAmbiguous;
        private readonly bool isInjectable;

       public SetInjectionPlan(INode name, ISet<InjectionPlan> entries) : base(name)
       {
            foreach (InjectionPlan ip in entries)
            {
                this.entries.Add(ip);
            }
            int numAlternatives = 1;
            bool isAmbiguous = false;
            bool isInjectable = true;
            foreach (InjectionPlan ip in entries) 
            {
                numAlternatives *= ip.GetNumAlternatives();
                isAmbiguous |= ip.IsAmbiguous();
                isInjectable &= ip.IsInjectable();
            }
            this.numAlternatives = numAlternatives;
            this.isAmbiguous = isAmbiguous;
            this.isInjectable = isInjectable;
        }

       public override int GetNumAlternatives()
       {
           return numAlternatives;
       }

       public override bool IsAmbiguous()
       {
           return isAmbiguous;
       }

       public override bool IsInjectable()
       {
           return isInjectable;
       }

       ////public override bool HasFutureDependency()
       ////{
       ////   return false;
       ////}

       public ISet<InjectionPlan> GetEntryPlans()
       {
           return new MonotonicHashSet<InjectionPlan>(this.entries);
       }

       public override string ToAmbiguousInjectString() 
        {
            StringBuilder sb = new StringBuilder(GetNode().GetFullName() + "(set) includes ambiguous plans [");
            foreach (InjectionPlan ip in entries) 
            {
                if (ip.IsAmbiguous()) 
                {
                    sb.Append("\n" + ip.ToAmbiguousInjectString());
                }
            }
            sb.Append("]");
            return sb.ToString();
        }

        public override string ToInfeasibleInjectString() 
        {
            StringBuilder sb = new StringBuilder(GetNode().GetFullName() + "(set) includes infeasible plans [");
            foreach (InjectionPlan ip in entries) 
            {
                if (!ip.IsFeasible()) 
                {
                    sb.Append("\n" + ip.ToInfeasibleInjectString());
                }
            }
            sb.Append("\n]");
            return sb.ToString();
        }

        public override bool IsInfeasibleLeaf()
        {
            return false;
        }

        public override string ToShallowString() 
        {
            StringBuilder sb = new StringBuilder("set { ");
            foreach (InjectionPlan ip in entries) 
            {
                sb.Append("\n" + ip.ToShallowString());
            }
            sb.Append("\n } ");
            return null;
        }
    }
}
