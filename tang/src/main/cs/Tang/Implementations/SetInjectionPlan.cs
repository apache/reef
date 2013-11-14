using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Types;
using Com.Microsoft.Tang.Util;

namespace Com.Microsoft.Tang.Implementations
{
    public class SetInjectionPlan : InjectionPlan
    {
        private readonly ISet<InjectionPlan> entries = new MonotonicSet<InjectionPlan>();
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

       public override bool HasFutureDependency()
       {
           return false;
       }

       public ISet<InjectionPlan> GetEntryPlans()
       {
           return new MonotonicSet<InjectionPlan>(this.entries);
       }

       public override string ToAmbiguousInjectString() 
        {
            StringBuilder sb = new StringBuilder(GetNode().GetFullName() + "(set) includes ambiguous plans [");
            foreach (InjectionPlan ip in entries) 
            {
                if(ip.IsAmbiguous()) 
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
                if(!ip.IsFeasible()) 
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
