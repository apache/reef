using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Exceptions;
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.Implementations
{
    public class Subplan : InjectionPlan
    {
        InjectionPlan[] alternatives;
        int numAlternatives;
        int selectedIndex;

        public Subplan(INode n, int selectedIndex, InjectionPlan[] alternatives)
            : base(n)
        {
            this.alternatives = alternatives;
            if (selectedIndex < -1 || selectedIndex >= alternatives.Length)
            {
                throw new IndexOutOfRangeException();
            }
            this.selectedIndex = selectedIndex;
            if (selectedIndex != -1)
            {
                this.numAlternatives = alternatives[selectedIndex].GetNumAlternatives();
            }
            else
            {
                int numAlternatives = 0;
                foreach (InjectionPlan a in alternatives)
                {
                    numAlternatives += a.GetNumAlternatives();
                }
                this.numAlternatives = numAlternatives;
            }
        }

        public ICollection<InjectionPlan> GetChildren()
        {
            return new ReadOnlyCollection<InjectionPlan>(this.alternatives.OfType<InjectionPlan>().ToList());
        }

        public Subplan(INode n, InjectionPlan[] alternatives)
            : this(n, -1, alternatives)
        {           
        }

        public override int GetNumAlternatives()
        {
            return this.numAlternatives;
        }

        public override bool IsAmbiguous()
        {
            if (selectedIndex == -1)
            {
                return true;
            }
            return alternatives[selectedIndex].IsAmbiguous();
        }

        public override bool IsInjectable()
        {
            if (selectedIndex == -1)
            {
                return false;
            }
            else
            {
                return alternatives[selectedIndex].IsInjectable();
            }
        }

        public override string ToString()
        {
            if (alternatives.Length == 1)
            {
                return GetNode().GetName() + " = " + alternatives[0];
            }
            else if (alternatives.Length == 0)
            {
                return GetNode().GetName() + ": no injectable constructors";
            }
            StringBuilder sb = new StringBuilder("[");
            sb.Append(GetNode().GetName() + " = " + alternatives[0]);
            for (int i = 1; i < alternatives.Length; i++)
            {
                sb.Append(" | " + alternatives[i]);
            }
            sb.Append("]");
            return sb.ToString();
        }

        public override string ToShallowString()
        {
            if (alternatives.Length == 1)
            {
                return GetNode().GetName() + " = " + alternatives[0].ToShallowString();
            }
            else if (alternatives.Length == 0)
            {
                return GetNode().GetName() + ": no injectable constructors";
            }
            StringBuilder sb = new StringBuilder("[");
            sb.Append(GetNode().GetName() + " = " + alternatives[0].ToShallowString());
            for (int i = 1; i < alternatives.Length; i++)
            {
                sb.Append(" | " + alternatives[i].ToShallowString());
            }
            sb.Append("]");
            return sb.ToString();
        }

        public int GetSelectedIndex()
        {
            return selectedIndex;
        }

        public InjectionPlan GetDelegatedPlan() 
        {
            if (selectedIndex == -1) 
            {
                throw new IllegalStateException("");
            } else 
            {
                return alternatives[selectedIndex];
            }
        }

        public override bool HasFutureDependency()
        {
            if (selectedIndex == -1)
            {
                throw new IllegalStateException("hasFutureDependency() called on ambiguous subplan!");
            }
            return alternatives[selectedIndex].HasFutureDependency();
        }

        public override string ToAmbiguousInjectString()
        {
            if (alternatives.Length == 1) 
            {
                return alternatives[0].ToAmbiguousInjectString();
            } 
            else if (selectedIndex != -1) 
            {
                return alternatives[selectedIndex].ToAmbiguousInjectString();
            } 
            else 
            {
                IList<InjectionPlan> alts = new List<InjectionPlan>();
                IList<InjectionPlan> ambig = new List<InjectionPlan>();
                foreach (InjectionPlan alt in alternatives) 
                {
                    if (alt.IsFeasible()) 
                    {
                        alts.Add(alt);
                    }
                    if (alt.IsAmbiguous()) 
                    {
                        ambig.Add(alt);
                    }
                }
                StringBuilder sb = new StringBuilder("Ambigous subplan " + GetNode().GetFullName());
                foreach (InjectionPlan alt in alts) 
                {
                    sb.Append("\n  " + alt.ToShallowString() + " ");
                }
                foreach (InjectionPlan alt in ambig) 
                {
                    sb.Append("\n  " + alt.ToShallowString() + " ");
                }
                sb.Append("\n]");
                return sb.ToString();
            }
        }

        public override string ToInfeasibleInjectString()
        {
            if (alternatives.Length == 1) 
            {
                return alternatives[0].ToInfeasibleInjectString();
            }
            else if (alternatives.Length == 0)
            {
                return "No known implementations / injectable constructors for "
                + this.GetNode().GetFullName();
            } 
            else if (selectedIndex != -1) 
            {
                return alternatives[selectedIndex].ToInfeasibleInjectString();
            } 
            else 
            {
                return "Multiple infeasible plans: " + ToPrettyString();
            }
        }

        public InjectionPlan[] GetPlans() 
        {
            InjectionPlan[] copy = new InjectionPlan[alternatives.Length];
            Array.Copy(alternatives, copy, alternatives.Length); // not sure if it does deep copy??
            return copy;
        }

        public override bool IsInfeasibleLeaf()
        {
            return false;
        }

    }
}
