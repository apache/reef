using System;

namespace Com.Microsoft.Tang.Exceptions
{
    public class ClassHierarchyException : Exception
    {
        public ClassHierarchyException(String msg) :  base(msg)
        {           
        }

        public ClassHierarchyException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
