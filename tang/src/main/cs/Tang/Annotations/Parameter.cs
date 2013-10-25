// ----------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------------
using System;
namespace Com.Microsoft.Tang.Annotations
{
    [System.AttributeUsage(System.AttributeTargets.Parameter)]
    public class Parameter : System.Attribute
    {
        public Type Value { get; set; } 
    }
}
