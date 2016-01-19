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
using System.Threading;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Util
{
    public class MonotonicTreeMap<TKey, TVal> : SortedDictionary<TKey, TVal> 
    {
        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

        private static readonly Logger LOGGER = Logger.GetLogger(typeof(MonotonicTreeMap<TKey, TVal>));

        public new void Add(TKey key, TVal value) 
        {
            _lock.EnterWriteLock();
            try
            {
                TVal val;
                if (TryGetValue(key, out val))
                {
                    var ex = new ArgumentException("Attempt to re-add: [" + key
                                                   + "]\n old value: " + val + " new value " + value);
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
                else
                {
                    base.Add(key, value);
                }
            }
            finally
            {
                if (_lock.IsWriteLockHeld)
                {
                    _lock.ExitWriteLock();
                }
            }
        }

        public new void Clear() // TODO
        {
            throw new NotSupportedException();
        }

        public new void Remove(TKey key) // TODO
        {
            throw new NotSupportedException();
        }
    }
}