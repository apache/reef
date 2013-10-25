// ----------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------------
using System;

namespace Com.Microsoft.Tang.Types
{
    public interface IConstructorArg
    {
        public String GetName();

        public String GetType();

        public bool IsInjectionFuture();

        public String GetNamedParameterName();  
    }
}
