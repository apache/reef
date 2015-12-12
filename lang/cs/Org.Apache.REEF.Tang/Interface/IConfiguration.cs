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
using Org.Apache.REEF.Tang.Types;

namespace Org.Apache.REEF.Tang.Interface
{
    public interface IConfiguration
    {
        IConfigurationBuilder newBuilder();
        string GetNamedParameter(INamedParameterNode np);
        IClassHierarchy GetClassHierarchy();

        ISet<object> GetBoundSet(INamedParameterNode np); // named parameter for a set
        IList<object> GetBoundList(INamedParameterNode np); // named parameter for a list

        IClassNode GetBoundConstructor(IClassNode cn);
        IClassNode GetBoundImplementation(IClassNode cn);
        IConstructorDef GetLegacyConstructor(IClassNode cn);

        ICollection<IClassNode> GetBoundImplementations();
        ICollection<IClassNode> GetBoundConstructors();
        ICollection<INamedParameterNode> GetNamedParameters();
        ICollection<IClassNode> GetLegacyConstructors();

        IEnumerator<KeyValuePair<INamedParameterNode, object>> GetBoundSets();
        IDictionary<INamedParameterNode, IList<object>> GetBoundList();
    }
}
