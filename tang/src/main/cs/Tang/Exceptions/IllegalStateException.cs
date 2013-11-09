using System;

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
