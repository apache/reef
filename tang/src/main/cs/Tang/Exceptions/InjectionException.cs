using System;

namespace Com.Microsoft.Tang.Exceptions
{
    public class InjectionException : Exception
    {
        public InjectionException(String msg)
            : base(msg)
        {           
        }

        public InjectionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
