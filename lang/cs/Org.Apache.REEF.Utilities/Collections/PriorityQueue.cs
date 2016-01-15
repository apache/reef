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
    /// A simple priority queue implementation, where the head of the queue is the 
    /// smallest element in the priority queue.
    /// </summary>
    public sealed class PriorityQueue<T> : ICollection<T> where T : IComparable<T>
    {
        private readonly List<T> _list;
 
        public PriorityQueue()
        {
            _list = new List<T>();
        }

        /// <summary>
        /// Gets the enumerator of the priority queue. The Enumerator returns elements
        /// in no guaranteed order.
        /// </summary>
        public IEnumerator<T> GetEnumerator()
        {
            return _list.GetEnumerator();
        }

        /// <summary>
        /// Gets the enumerator of the priority queue. The Enumerator returns elements
        /// in no guaranteed order.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return _list.GetEnumerator();
        }

        /// <summary>
        /// Peeks at the head of the priority queue, but does not remove the element
        /// at the head. 
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when the priority queue is empty</exception>
        public T Peek()
        {
            if (_list.Count == 0)
            {
                throw new InvalidOperationException("The PriorityQueue is empty.");
            }

            return _list[0];
        }

        /// <summary>
        /// Adds an element to the priority queue.
        /// </summary>
        public void Enqueue(T item)
        {
            Add(item);
        }

        /// <summary>
        /// Dequeues an item from the priority queue. Removes the head of the priority queue, which
        /// is the smallest element in the priority queue.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when the priority queue is empty</exception>
        public T Dequeue()
        {
            if (_list.Count == 0)
            {
                throw new InvalidOperationException("Cannot remove from an empty PriorityQueue.");
            }

            // Removes element and places last to top
            var ret = _list[0];
            _list[0] = _list[_list.Count - 1];
            _list.RemoveAt(_list.Count - 1);

            if (_list.Count == 0)
            {
                return ret;
            }

            // Percolate down.
            var idx = 0;
            var item = _list[0];
            var lchildIdx = GetLeftChildIndex(idx);

            while (lchildIdx < _list.Count)
            {
                // Find the smaller child to compare
                int smallerIdx;
                var rchildIdx = lchildIdx + 1;

                if (rchildIdx < _list.Count)
                {
                    smallerIdx = _list[lchildIdx].CompareTo(_list[rchildIdx]) <= 0 ? lchildIdx : rchildIdx;
                }
                else
                {
                    smallerIdx = lchildIdx;
                }

                // Compare with the smaller child, swap down if greater
                if (item.CompareTo(_list[smallerIdx]) > 0)
                {
                    _list[idx] = _list[smallerIdx];
                    _list[smallerIdx] = item;
                    idx = smallerIdx;
                    lchildIdx = GetLeftChildIndex(idx);
                }
                else
                {
                    // Order holds
                    break;
                }
            }

            return ret;
        }

        /// <summary>
        /// <see cref="Enqueue"/>.
        /// </summary>
        public void Add(T item)
        {
            // Add item
            _list.Add(item);
            var idx = _list.Count - 1;

            // Percolate up
            while (idx > 0)
            {
                var parentIdx = (idx - 1) / 2;

                // Swap up if parent is greater
                if (_list[parentIdx].CompareTo(item) > 0)
                {
                    _list[idx] = _list[parentIdx];
                    _list[parentIdx] = item;
                    idx = parentIdx;
                }
                else
                {
                    // Order holds
                    break;
                }
            }
        }

        /// <summary>
        /// Clears the priority queue.
        /// </summary>
        public void Clear()
        {
            _list.Clear();
        }

        /// <summary>
        /// Checks if the list contains an item.
        /// </summary>
        public bool Contains(T item)
        {
            return _list.Contains(item);
        }

        /// <summary>
        /// Copies the priority queue to a compatible array, starting at the array's 
        /// provided index.
        /// </summary>
        public void CopyTo(T[] array, int arrayIndex)
        {
            _list.CopyTo(array, arrayIndex);
        }

        /// <summary>
        /// Remove is not supported.
        /// </summary>
        /// <exception cref="NotSupportedException">Operation not supported</exception>
        public bool Remove(T item)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Returns the count of the priority queue.
        /// </summary>
        public int Count
        {
            get
            {
                return _list.Count;
            }
        }

        /// <summary>
        /// Always returns false.
        /// </summary>
        public bool IsReadOnly
        {
            get { return false; } 
        }

        private static int GetLeftChildIndex(int idx)
        {
            return (2 * idx) + 1;
        }
    }
}
