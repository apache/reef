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
using System.Collections;
using System.Collections.Generic;

namespace Org.Apache.REEF.Utilities.Collections
{
    /// <summary>
    /// An implementation of a set that is read only.
    /// Backed by a HashSet.
    /// </summary>
    public sealed class ReadOnlySet<T> : IReadOnlyCollection<T>, ISet<T>
    {
        private readonly ISet<T> _backingSet;

        public ReadOnlySet(IEnumerable<T> enumerable)
        {
            _backingSet = new HashSet<T>(enumerable);
        }

        public IEnumerator<T> GetEnumerator()
        {
            return _backingSet.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        void ICollection<T>.Add(T item)
        {
            throw new NotSupportedException("ReadOnlySet does not support Add.");
        }

        public bool Add(T item)
        {
            throw new NotSupportedException("ReadOnlySet does not support Add.");
        }

        public void UnionWith(IEnumerable<T> other)
        {
            throw new NotSupportedException("ReadOnlySet does not support UnionWith.");
        }

        public void IntersectWith(IEnumerable<T> other)
        {
            throw new NotSupportedException("ReadOnlySet does not support IntersectWith.");
        }

        public void ExceptWith(IEnumerable<T> other)
        {
            throw new NotSupportedException("ReadOnlySet does not support ExceptWith.");
        }

        public void SymmetricExceptWith(IEnumerable<T> other)
        {
            throw new NotSupportedException("ReadOnlySet does not support SymmetricExceptWith.");
        }

        public bool IsSubsetOf(IEnumerable<T> other)
        {
            return _backingSet.IsSubsetOf(other);
        }

        public bool IsSupersetOf(IEnumerable<T> other)
        {
            return _backingSet.IsSupersetOf(other);
        }

        public bool IsProperSupersetOf(IEnumerable<T> other)
        {
            return _backingSet.IsProperSupersetOf(other);
        }

        public bool IsProperSubsetOf(IEnumerable<T> other)
        {
            return _backingSet.IsProperSubsetOf(other);
        }

        public bool Overlaps(IEnumerable<T> other)
        {
            return _backingSet.Overlaps(other);
        }

        public bool SetEquals(IEnumerable<T> other)
        {
            return _backingSet.SetEquals(other);
        }

        public void Clear()
        {
            throw new NotSupportedException("ReadOnlySet does not support Clear.");
        }

        public bool Contains(T item)
        {
            return _backingSet.Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            _backingSet.CopyTo(array, arrayIndex);
        }

        public bool Remove(T item)
        {
            throw new NotSupportedException("ReadOnlySet does not support Remove.");
        }

        public int Count
        {
            get { return _backingSet.Count; }
        }

        public bool IsReadOnly
        {
            get { return true; }
        }
    }
}
