// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Types
{
    public interface INamedParameterNode : INode
    {
        string GetDocumentation();

        string GetShortName();

        string[] GetDefaultInstanceAsStrings();

        string GetSimpleArgName();

        string GetFullArgName();

        bool IsSet();

        bool IsList();

        /// <summary>
        /// It returns an alias of the NamedParameter
        /// The alias is defined as an attribute of the NamedParameter
        /// </summary>
        /// <returns></returns>
        string GetAlias();

        /// <summary>
        /// It returns the programming language for the alias
        /// </summary>
        /// <returns></returns>
        Language GetAliasLanguage();
    }
}
