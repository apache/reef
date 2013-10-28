// ----------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------------
using System;

namespace Com.Microsoft.Tang.Types
{
    public interface IConstructorArg
    {
        String GetName();

        String Gettype();

        bool IsInjectionFuture();

        String GetNamedParameterName();  
    }
}
