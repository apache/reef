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
using System.Linq;
using System.Threading;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Util
{
    public class MonotonicHashSet<T> : HashSet<T>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(MonotonicHashSet<T>));
        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

        public MonotonicHashSet()
        {           
        }

        public MonotonicHashSet(IEnumerable<T> collection)
            : base(collection)
        {
        }

        public MonotonicHashSet(T[] collection)
            : base(collection.ToList())
        {
        }

        public new bool Add(T e)
        {
            _lock.EnterWriteLock();

            try
            {
                if (Contains(e))
                {
                    var ex = new ArgumentException("Attempt to re-add " + e
                                                   + " to MonotonicSet!");
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
                return base.Add(e);
            }
            finally
            {
                if (_lock.IsWriteLockHeld)
                {
                    _lock.ExitWriteLock();
                }
            }
        }

        public bool AddAll(ICollection<T> c)
        {
            _lock.EnterWriteLock();
            try
            {
                foreach (T t in c)
                {
                    if (Contains(t))
                    {
                        var ex = new ArgumentException("Attempt to re-add " + t
                                                       + " to MonotonicSet!");
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                    }
                    base.Add(t);
                }
            }
            finally
            {
                if (_lock.IsWriteLockHeld)
                {
                    _lock.ExitWriteLock();
                }
            }
            return c.Count != 0;
        }

        public bool ContainsAll(ICollection<T> c)
        {
            _lock.EnterReadLock();

            try
            {
                foreach (T t in c)
                {
                    if (!Contains(t))
                    {
                        return false;
                    }
                }
            }
            finally
            {
                if (_lock.IsReadLockHeld)
                {
                    _lock.ExitReadLock();
                }
            }
            return true;
        }

        public new void Clear() 
        {
            Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new NotSupportedException("Attempt to clear MonotonicSet!"), LOGGER);
        }
 
        public bool Remove(object o) 
        {
            Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new NotSupportedException("Attempt to remove " + o + " from MonotonicSet!"), LOGGER);
            return false;
        }
    }
}