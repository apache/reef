// ----------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------------
using System;

namespace Com.Microsoft.Tang.Types
{
    public interface ConstructorDef<T> : IComparable
    {
        public String GetClassName();

        public IConstructorArg[] GetArgs();

        public bool IsMoreSpecificThan(ConstructorDef<T> def);

        public bool TakesParameters(IClassNode<T>[] paramTypes);
    }
}
