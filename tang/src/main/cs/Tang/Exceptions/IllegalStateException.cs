using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Exceptions
{
    public class IllegalStateException : Exception
    {
        public IllegalStateException(String msg)
            : base(msg)
        {           
        }

        public IllegalStateException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
