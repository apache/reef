// ----------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------------
using System;

namespace Com.Microsoft.Tang.Types
{
    public interface INamedParameterNode
    {
        public String GetDocumentation();

        public String GetShortName();

        public String[] GetDefaultInstanceAsStrings();

        public String GetSimpleArgName();

        public String GetFullArgName();

        public bool IsSet();
    }
}
