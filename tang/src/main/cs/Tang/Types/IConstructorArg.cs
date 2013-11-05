using System;

namespace Com.Microsoft.Tang.Types
{
    public interface IConstructorArg
    {
        string GetName();

        string Gettype();

        bool IsInjectionFuture();

        string GetNamedParameterName();  
    }
}
