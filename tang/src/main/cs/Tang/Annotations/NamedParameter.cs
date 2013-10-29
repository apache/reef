// ----------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------------
using System;

namespace Com.Microsoft.Tang.Annotations
{
    [System.AttributeUsage(System.AttributeTargets.Class)]
    public class NamedParameterAttribute : System.Attribute
    {
        public string Documentation { get; set; }
        public Type ArgClass { get; set; }
        public string DefaultInstance { get; set; }
        public string ShortName { get; set; }
        public NamedParameterAttribute(Type argClass, string documentation = "", string defaultInstance = "", string shortName = "")
        {
            this.ArgClass = argClass;
            this.Documentation = documentation;
            this.DefaultInstance = defaultInstance;
            this.ShortName = shortName;
        }
    }
}
