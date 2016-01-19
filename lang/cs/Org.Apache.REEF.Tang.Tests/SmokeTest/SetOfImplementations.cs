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

using System.Collections.Generic;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Tang.Tests.SmokeTest
{
    public class SetOfImplementations 
    {
        private readonly ISet<ISetInterface> theInstances;

        [Inject]
        SetOfImplementations([Parameter(typeof(TestConfigurationModuleBuilder.SetOfInstances))] ISet<ISetInterface> theInstances) 
        {
            this.theInstances = theInstances;
        }

        public override bool Equals(object o) 
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || !(o is SetOfImplementations))
            {
                return false;
            }

            SetOfImplementations that = (SetOfImplementations)o;

            if (that.theInstances.Count != this.theInstances.Count)
            {
                return false;
            }

            if (!Utilities.Utilities.Equals<ISetInterface>(theInstances, that.theInstances))
            {
                return false;
            }

            return true;
        }

        public override int GetHashCode() 
        {
            return theInstances.GetHashCode();
        }

        public bool isValid() 
        {
            return this.theInstances.Count == 2;
        }
    }
}