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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Implementations.ClassHierarchy
{
    public class NamedParameterNodeImpl : AbstractNode, INamedParameterNode
    {
        private readonly String fullArgName;
        private readonly String simpleArgName;
        private readonly String documentation;
        private readonly String shortName;
        private readonly String[] defaultInstanceAsStrings;
        private readonly bool isSet;
        private readonly bool isList;
        private readonly string alias;
        private readonly Language aliasLanguage;

        public NamedParameterNodeImpl(INode parent, String simpleName,
            String fullName, String fullArgName, String simpleArgName, bool isSet, bool isList,
            String documentation, String shortName, String[] defaultInstanceAsStrings, string alias = null, Language aliasLanguage = Language.Cs)
            : base(parent, simpleName, fullName)
        {
            this.fullArgName = fullArgName;
            this.simpleArgName = simpleArgName;
            this.isSet = isSet;
            this.isList = isList;
            this.documentation = documentation;
            this.shortName = shortName;
            this.defaultInstanceAsStrings = defaultInstanceAsStrings;
            this.alias = alias;
            this.aliasLanguage = aliasLanguage;
        }

        public override String ToString()
        {
            return GetSimpleArgName() + " " + GetName();
        }

        public String GetSimpleArgName()
        {
            return simpleArgName;
        }

        public String GetFullArgName()
        {
            return fullArgName;
        }

        public String GetDocumentation()
        {
            return documentation;
        }

        public String GetShortName()
        {
            return shortName;
        }

        public String[] GetDefaultInstanceAsStrings()
        {
            return defaultInstanceAsStrings;
        }

        public bool IsSet()
        {
            return isSet;
        }

        public bool IsList()
        {
            return isList;
        }

        public string GetAlias()
        {
            return alias;
        }

        public Language GetAliasLanguage()
        {
            return aliasLanguage;
        }
    }
}
