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
    public class SetOfBaseTypes
    {
        private readonly ISet<int> integers;
        private readonly ISet<double> doubles;
        private readonly ISet<string> strings;
        private readonly ISet<int> moreIntegers;

        [Inject]
        private SetOfBaseTypes([Parameter(typeof(Integers))] ISet<int> integers,
                               [Parameter(typeof(Doubles))] ISet<double> doubles,
                               [Parameter(typeof(Strings))] ISet<string> strings,
                               [Parameter(typeof(MoreIntegers))] ISet<int> moreIntegers)
        {
            this.integers = integers;
            this.doubles = doubles;
            this.strings = strings;
            this.moreIntegers = moreIntegers;
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || !(o is SetOfBaseTypes))
            {
                return false;
            }

            SetOfBaseTypes that = (SetOfBaseTypes)o;

            if (!Utilities.Utilities.Equals(doubles, that.doubles))
            {
                return false;
            }

            if (!Utilities.Utilities.Equals(integers, that.integers))
            {
                return false;
            }

            if (!Utilities.Utilities.Equals(strings, that.strings))
            {
                return false;
            }

            return true;
        }
        
        public override int GetHashCode()
        {
            int result = integers.GetHashCode();
            result = (31 * result) + doubles.GetHashCode();
            result = (31 * result) + strings.GetHashCode();
            return result;
        }

        [NamedParameter]
        public class Integers : Name<ISet<int>>
        {
        }

        [NamedParameter(DefaultValues = new string[] { "1", "2", "3" })]
        public class MoreIntegers : Name<ISet<int>>
        {
        }

        [NamedParameter]
        public class Doubles : Name<ISet<double>>
        {
        }

        [NamedParameter]
        public class Strings : Name<ISet<string>>
        {
        }
    }
}