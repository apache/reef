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
        private readonly string fullArgName;
        private readonly string simpleArgName;
        private readonly string documentation;
        private readonly string shortName;
        private readonly string[] defaultInstanceAsStrings;
        private readonly bool isSet;
        private readonly bool isList;
        private readonly string alias;
        private readonly Language aliasLanguage;

        public NamedParameterNodeImpl(INode parent, string simpleName,
            string fullName, string fullArgName, string simpleArgName, bool isSet, bool isList,
            string documentation, string shortName, string[] defaultInstanceAsStrings, string alias = null, Language aliasLanguage = Language.Cs)
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

        public override string ToString()
        {
            return GetSimpleArgName() + " " + GetName();
        }

        public string GetSimpleArgName()
        {
            return simpleArgName;
        }

        public string GetFullArgName()
        {
            return fullArgName;
        }

        public string GetDocumentation()
        {
            return documentation;
        }

        public string GetShortName()
        {
            return shortName;
        }

        public string[] GetDefaultInstanceAsStrings()
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
