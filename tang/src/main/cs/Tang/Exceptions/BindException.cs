using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Exceptions
{
    public class BindException : Exception
    {
        private static readonly long serialVersionUID = 1L;
        public BindException(String msg)
            : base(msg)
        {           
        }
    }
}
