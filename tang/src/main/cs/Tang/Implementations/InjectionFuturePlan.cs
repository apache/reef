using System;
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
