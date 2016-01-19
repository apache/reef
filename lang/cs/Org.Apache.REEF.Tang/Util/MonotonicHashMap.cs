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

using System;
using System.Collections.Generic;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Util
{
    public class MonotonicHashMap<T, U> : Dictionary<T, U>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(MonotonicHashMap<T, U>));

        public new void Add(T key, U value)
        {
            U old;
            TryGetValue(key, out old);
            if (old != null)
            {
                var ex = new ArgumentException("Attempt to re-add: [" + key + "] old value: " + old + " new value " + value);
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
            base.Add(key, value);
        }

        public void AddAll(IDictionary<T, U> m) 
        {
            foreach (T t in m.Keys) 
            {
                if (ContainsKey(t)) 
                {
                   U old;
                   m.TryGetValue(t, out old);
                   Add(t, old); // guaranteed to throw.
                }
            }
            foreach (T t in m.Keys) 
            {
                U old;
                m.TryGetValue(t, out old);
                Add(t, old);
            }
        }

        public bool IsEmpty()
        {
            return Count == 0;
        }

        public U Get(T key)
        {
            U val;
            TryGetValue(key, out val);
            return val;
        }

        public new void Clear() 
        {
            throw new NotSupportedException();
        }

        public new bool Remove(T key)
        {
            throw new NotSupportedException();
        }
    }
}
