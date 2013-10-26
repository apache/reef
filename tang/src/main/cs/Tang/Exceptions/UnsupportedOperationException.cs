using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Exceptions
{
    class UnsupportedOperationException : Exception
    {
        public UnsupportedOperationException(String msg)
            : base(msg)
        {           
        }
    }
}
