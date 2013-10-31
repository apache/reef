using System;
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

        public String GetName()
        {
            return name == null ? type : name;
        }

        public String GetNamedParameterName()
        {
            return name;
        }

        public String Gettype()
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
