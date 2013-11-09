using System;

namespace Com.Microsoft.Tang.Exceptions
{
    public class NameResolutionException : BindException
    {
        private static readonly long serialVersionUID = 1L;
        public NameResolutionException(String name, String longestPrefix) :
            base(string.Format("Could not resolve {0}.  Search ended at prefix {1}. This can happen due to typos in class names that are passed as strings, or because Tang uses Assembly loader other than the one that generated the class reference ((make sure you use the full name of a class)",
                name, longestPrefix))
        {
        }

        public NameResolutionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
