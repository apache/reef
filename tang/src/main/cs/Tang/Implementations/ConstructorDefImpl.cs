using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Exceptions;
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.Implementations
{
    public class ConstructorDefImpl<T> : IConstructorDef<T>
    {
        private readonly IConstructorArg[] args;
        private readonly String className;

        public ConstructorDefImpl(String className, IConstructorArg[] args, bool injectable)  
        {
            this.className = className;
            this.args = args;
            if (injectable) {
                for (int i = 0; i < this.GetArgs().Length; i++) {
                    for (int j = i + 1; j < this.GetArgs().Length; j++) {
                        if (this.GetArgs()[i].Equals(this.GetArgs()[j])) {
                            throw new ClassHierarchyException(
                                "Repeated constructor parameter detected.  "
                                + "Cannot inject constructor " + ToString());
                        }
                    }
                }
            }
        }

        public IConstructorArg[] GetArgs() 
        {
            return args;
        }

        public String GetClassName() 
        {
            return className;
        }

        private String Join(String sep, Object[] vals)
        {
            if (vals.Length != 0)
            {
                StringBuilder sb = new StringBuilder(vals[0].ToString());
                for (int i = 1; i < vals.Length; i++)
                {
                    sb.Append(sep + vals[i]);
                }
                return sb.ToString();
            }
            else
            {
                return "";
            }
        }

        public override String ToString()
        {
            StringBuilder sb = new StringBuilder(className);
            sb.Append("(");
            sb.Append(Join(",", args));
            sb.Append(")");
            return sb.ToString();
        }

        // Return true if our list of args is a superset of those in def.
        public bool IsMoreSpecificThan(IConstructorDef<T> def)
        {    
            // Is everything in def also in this?
            for (int i = 0; i < def.GetArgs().Length; i++)
            {
                bool found = false;
                for (int j = 0; j < this.GetArgs().Length; j++)
                {
                    if (GetArgs()[j].Equals(def.GetArgs()[i]))
                    {
                        found = true;
                        break;
                    }
                }
                // If not, then argument j from def is not in our list.  Return false.
                if (found == false)
                    return false;
            }
            // Everything in def's arg list is in ours.  Do we have at least one extra
            // argument?
            return GetArgs().Length > def.GetArgs().Length;
        }

        public bool TakesParameters(IList<IClassNode<T>> paramTypes)
        {
            if (paramTypes.Count != args.Length) 
            {
                return false;
            }

            int i = 0;
            foreach (IClassNode<T> t in paramTypes)
            {
                if (!args[i].Gettype().Equals(paramTypes[i].GetFullName()))
                {
                    return false;
                }
                else
                {
                    i++;
                }

            }
            //for (int i = 0; i < paramTypes.Length; i++) {
            //    if (!args[i].Gettype().Equals(paramTypes[i].GetFullName())) {
            //        return false;
            //    }
            //}
            return true;
        }

        public override bool Equals(Object o) 
        {
            return EqualsIgnoreOrder((IConstructorDef<T>) o);
        }

        private bool EqualsIgnoreOrder(IConstructorDef<T> def) 
        {
            if (GetArgs().Length != def.GetArgs().Length) 
            {
                return false;
            }
            for (int i = 0; i < GetArgs().Length; i++) 
            {
                bool found = false;
                for (int j = 0; j < GetArgs().Length; j++) 
                {
                    if (GetArgs()[i].GetName().Equals(GetArgs()[j].GetName())) 
                    {
                        found = true;
                    }
                }
                if (!found) 
                {
                    return false;
                }
            }
            return true;
        }

        public int CompareTo(object obj)
        {
            IConstructorDef<T> o = (IConstructorDef<T>)obj;
            return ToString().CompareTo(o.ToString());
        }
    }
}
