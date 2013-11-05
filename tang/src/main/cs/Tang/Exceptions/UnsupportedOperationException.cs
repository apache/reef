using System;

namespace Com.Microsoft.Tang.Exceptions
{
    class UnsupportedOperationException : Exception
    {
        public UnsupportedOperationException(String msg)
            : base(msg)
        {           
        }

        public UnsupportedOperationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
