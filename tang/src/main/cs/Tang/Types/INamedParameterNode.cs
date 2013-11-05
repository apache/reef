// ----------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------------
using System;

namespace Com.Microsoft.Tang.Types
{
    public interface INamedParameterNode : INode
    {
        String GetDocumentation();

        String GetShortName();

        String[] GetDefaultInstanceAsStrings();

        String GetSimpleArgName();

        String GetFullArgName();

        bool IsSet();
    }
}
