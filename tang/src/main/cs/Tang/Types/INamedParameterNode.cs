using System;

namespace Com.Microsoft.Tang.Types
{
    public interface INamedParameterNode : INode
    {
        string GetDocumentation();

        string GetShortName();

        string[] GetDefaultInstanceAsStrings();

        string GetSimpleArgName();

        string GetFullArgName();

        bool IsSet();
    }
}
