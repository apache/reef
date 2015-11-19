/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Annotations
{
    [System.AttributeUsage(System.AttributeTargets.Class)]
    public class NamedParameterAttribute : System.Attribute
    {
        public const string ReefUninitializedValue = "__REEF_UNINITIALIZED_VALUE__";

        public string Documentation { get; set; }
        public string ShortName { get; set; }
        public string DefaultValue { get; set; }
        public Type DefaultClass { get; set; }
        public string[] DefaultValues { get; set; }
        public Type[] DefaultClasses { get; set; }
        public string Alias { get; set; }
        public Language AliasLanguage { get; set; }

        public NamedParameterAttribute(string documentation = "", string shortName = "",
            string defaultValue = ReefUninitializedValue, Type defaultClass = null, string[] defaultValues = null, Type[] defaultClasses = null, string alias = null, Language aliasLanguage = Language.Cs)
        {
            this.Documentation = documentation;
            this.ShortName = shortName;
            this.DefaultValue = defaultValue;
            this.DefaultClass = defaultClass;
            this.DefaultValues = defaultValues;
            this.DefaultClasses = defaultClasses;
            this.Alias = alias;
            this.AliasLanguage = aliasLanguage;
        }
    }
}