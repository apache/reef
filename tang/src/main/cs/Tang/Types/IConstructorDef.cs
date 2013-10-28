// ----------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------------
using System;
using System.Collections.Generic;

namespace Com.Microsoft.Tang.Types
{
    public interface IConstructorDef<T> : IComparable
    {
        String GetClassName();

        IConstructorArg[] GetArgs();

        bool IsMoreSpecificThan(IConstructorDef<T> def);

        bool TakesParameters(IList<IClassNode<T>> paramTypes);
    }
}
