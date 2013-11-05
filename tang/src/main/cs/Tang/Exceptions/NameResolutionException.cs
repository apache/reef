using System;

namespace Com.Microsoft.Tang.Exceptions
{
    public class NameResolutionException : BindException
    {
        private static readonly long serialVersionUID = 1L;
        public NameResolutionException(String name, String longestPrefix) : 
            base("Could not resolve " + name + ".  Search ended at prefix " + longestPrefix + 
            " This can happen due to typos in class names that are passed as strings, or because Tang is configured to use a classloader other than the one that generated the class reference (check your classpath and the code that instantiated Tang)")
        {
        }

        public NameResolutionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
