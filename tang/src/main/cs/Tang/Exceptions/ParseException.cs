using System;

namespace Com.Microsoft.Tang.Exceptions
{
    public class ParseException : BindException
    {
        private static readonly long serialVersionUID = 1L;
        public ParseException(String message)
            : base(message)
        {           
        }

        public ParseException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
