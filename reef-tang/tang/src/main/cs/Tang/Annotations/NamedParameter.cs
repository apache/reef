/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
﻿using System;

namespace Com.Microsoft.Tang.Annotations
{
    [System.AttributeUsage(System.AttributeTargets.Class)]
    public class NamedParameterAttribute : System.Attribute
    {
        public string Documentation { get; set; }
        public string ShortName { get; set; }
        public string DefaultValue { get; set; }
        public Type DefaultClass { get; set; }
        public string[] DefaultValues { get; set; }
        public Type[] DefaultClasses { get; set; }

        public NamedParameterAttribute(string documentation = "", string shortName = "",
            string defaultValue = "", Type defaultClass = null, string[] defaultValues = null, Type[] defaultClasses = null)
        {
            this.Documentation = documentation;
            this.ShortName = shortName;
            this.DefaultValue = defaultValue;
            this.DefaultClass = defaultClass;
            this.DefaultValues = defaultValues;
            this.DefaultClasses = defaultClasses;
        }
    }
}
