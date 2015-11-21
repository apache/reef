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
using System.Linq;
using System.Threading;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Util
{
    public abstract class AbstractMonotonicMultiMap<K, V> : ICollection<KeyValuePair<K, V>>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(AbstractMonotonicMultiMap<K, V>));

        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

        private readonly IDictionary<K, ISet<V>> map;

        private int size = 0;

        public AbstractMonotonicMultiMap(IDictionary<K, ISet<V>> map)
        {
            this.map = map;
        }

        public ICollection<K> Keys
        {
            get { return map.Keys; }
        }

        public int Count
        {
            get { return size; }
        }

        public bool IsReadOnly
        {
            get { throw new NotImplementedException(); }
        }

        public void Add(K key, V val)
        {
            _lock.EnterWriteLock();
            try
            {
                ISet<V> vals;
                map.TryGetValue(key, out vals);

                if (vals == null)
                {
                    vals = new MonotonicHashSet<V>();
                    map.Add(key, vals);
                }
                vals.Add(val);
                size++;
            }
            finally
            {
                if (_lock.IsWriteLockHeld)
                {
                    _lock.ExitWriteLock();
                }
            }
        }

        public ISet<V> GetValuesForKey(K key)
        {
            _lock.EnterReadLock();
            try
            {
                ISet<V> ret;
                map.TryGetValue(key, out ret);
                if (ret == null)
                {
                    return new MonotonicHashSet<V>();
                }
                return ret;
            }
            finally
            {
                if (_lock.IsReadLockHeld)
                {
                    _lock.ExitReadLock();
                }
            }
        }

        public bool Contains(K key, V val)
        {
            _lock.EnterReadLock();
            try
            {
                ISet<V> vals;
                map.TryGetValue(key, out vals);

                if (vals != null)
                {
                    return vals.Contains(val);
                }
                return false;
            }
            finally
            {
                if (_lock.IsReadLockHeld)
                {
                    _lock.ExitReadLock();
                }
            }
        }

        public bool Add(KeyValuePair<K, V> e)
        {
            Add(e.Key, e.Value);
            return true;
        }

        public bool AddAll(ICollection<KeyValuePair<K, V>> c) // where T : KeyValuePair<K, V>
        {
            bool ret = false;
            foreach (KeyValuePair<K, V> e in c) 
            {
                Add(e);
                ret = true;
            }
            return ret;
        }

        public void Clear()
        {
            throw new NotSupportedException("MonotonicMultiMap cannot be cleared!");
        }

        public bool Contains(object o) 
        {
            KeyValuePair<K, V> e = (KeyValuePair<K, V>)o;
            return Contains((K)e.Key, (V)e.Value);
        }

        public bool ContainsAll<T>(ICollection<T> c) 
        {
            foreach (object o in c) 
            {
                if (!Contains(o)) 
                { 
                    return false; 
                }
            }
            return true;
        }

        public bool IsEmpty()
        {
            return size == 0;
        }

        public ISet<V> Values() 
        {
            _lock.EnterReadLock();

            try
            {
                ISet<V> s = new HashSet<V>();
                foreach (KeyValuePair<K, V> e in this)
                {
                    s.Add(e.Value);
                }
                return s;
            }
            finally
            {
                if (_lock.IsReadLockHeld)
                {
                    _lock.ExitReadLock();
                }
            }
        }

        public int Size()
        {
            return size;
        }

        public bool ContainsKey(K k)
        {
            _lock.EnterReadLock();

            try
            {
                if (map.ContainsKey(k))
                {
                    return GetValuesForKey(k).Count != 0;
                }
                return false;
            }
            finally 
            {                
                if (_lock.IsReadLockHeld)
                {
                    _lock.ExitReadLock();
                }
            }
        }

        void ICollection<KeyValuePair<K, V>>.Add(KeyValuePair<K, V> item)
        {
            throw new NotImplementedException();
        }

        public void CopyTo(KeyValuePair<K, V>[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }

        public bool Remove(KeyValuePair<K, V> item)
        {
            throw new NotImplementedException();
        }

        public bool Contains(KeyValuePair<K, V> item)
        {
            throw new NotImplementedException();
        }

        public IEnumerator<KeyValuePair<K, V>> GetEnumerator()
        {
            _lock.EnterReadLock();

            try
            {
                return map.SelectMany(kvp => kvp.Value, (kvp, v) => new KeyValuePair<K, V>(kvp.Key, v)).GetEnumerator();
            }
            finally
            {
                if (_lock.IsReadLockHeld)
                {
                    _lock.ExitReadLock();
                }
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }
}