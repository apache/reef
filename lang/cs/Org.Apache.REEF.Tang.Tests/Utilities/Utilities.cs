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

namespace Org.Apache.REEF.Tang.Tests.Utilities
{
    public class Utilities
    {
        public static bool Equals<T>(ICollection<T> s1, ISet<T> s2)
        {
            if (s1 == s2)
            {
                return true;
            }
            if (s1 == null || s2 == null)
            {
                return false;
            }
            if (s1.Count != s2.Count)
            {
                return false;
            }
            foreach (T t in s1)
            {
                if (!Contains<T>(s2, t))
                {
                    return false;
                }
            }
            return true;
        }

        public static bool Contains<T>(ICollection<T> s, T t)
        {
            foreach (T t1 in s)
            {
                if (t1.Equals(t))
                {
                    return true;
                }
            }
            return false;
        }
    }
}