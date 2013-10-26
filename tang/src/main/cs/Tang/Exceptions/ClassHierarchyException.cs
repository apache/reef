using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Exceptions
{
    public class ClassHierarchyException : Exception
    {
        public ClassHierarchyException(String msg) :  base(msg)
        {           
        }
    }
}
