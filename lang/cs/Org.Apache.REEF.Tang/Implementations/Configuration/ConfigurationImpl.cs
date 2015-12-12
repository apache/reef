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
using System.Collections.Generic;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Types;

namespace Org.Apache.REEF.Tang.Implementations.Configuration
{
    public class ConfigurationImpl : IConfiguration
    {
        public readonly ConfigurationBuilderImpl Builder;

        public ConfigurationImpl(ConfigurationBuilderImpl builder)
        {
            Builder = builder;
        }

        public IClassHierarchy GetClassHierarchy()
        {
            return Builder.ClassHierarchy;
        }

        public IConfigurationBuilder newBuilder()
        {
            return ((ConfigurationImpl)Builder.Build()).Builder;
        }
           
        public ICollection<IClassNode> GetBoundImplementations()
        {
            return Builder.BoundImpls.Keys;
        }

        public IClassNode GetBoundImplementation(IClassNode cn)
        {
            IClassNode v;

            Builder.BoundImpls.TryGetValue(cn, out v);

            return v;
        }

        public ICollection<IClassNode> GetBoundConstructors()
        {
            return Builder.BoundConstructors.Keys;
        }

        public IClassNode GetBoundConstructor(IClassNode cn)
        {
            IClassNode v;

            Builder.BoundConstructors.TryGetValue(cn, out v);

            return v;
        }

        public ICollection<INamedParameterNode> GetNamedParameters()
        {
            return Builder.NamedParameters.Keys;
        }

        public string GetNamedParameter(INamedParameterNode np)
        {
            string v = null;
            Builder.NamedParameters.TryGetValue(np, out v);

            return v;
        }

        public IConstructorDef GetLegacyConstructor(IClassNode cn)
        {
            IConstructorDef v;

            Builder.LegacyConstructors.TryGetValue(cn, out v);

            return v;
        }

        public ICollection<IClassNode> GetLegacyConstructors()
        {
            return Builder.LegacyConstructors.Keys;
        }

        public ISet<object> GetBoundSet(INamedParameterNode np) 
        {
            return Builder.BoundSetEntries.GetValuesForKey(np);
        }

        public IEnumerator<KeyValuePair<INamedParameterNode, object>> GetBoundSets() 
        {
            return Builder.BoundSetEntries.GetEnumerator();
        }

        public IDictionary<INamedParameterNode, IList<object>> GetBoundList()
        {
            return Builder.BoundLists;
        }

        public IList<object> GetBoundList(INamedParameterNode np)
        {
            IList<object> list;
            Builder.BoundLists.TryGetValue(np, out list);
            return list;
        }
    }
}