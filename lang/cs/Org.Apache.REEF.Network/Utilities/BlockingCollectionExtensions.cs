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
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Utilities
{
    public static class BlockingCollectionExtensions
    {
        /// <summary>
        /// Removes the given item from the BlockingCollection if it is present.
        /// If it is not present, it blocks until any item is available in the 
        /// BlockingCollection.  It then removes and returns that first available
        /// item.
        /// </summary>
        /// <typeparam name="T">The type of BlockingCollection</typeparam>
        /// <param name="collection">The BlockingCollection to remove the specified item</param>
        /// <param name="item">The item to remove from the BlockingCollection, if it exists</param>
        /// <returns>The specified item, or the first available item if the specified item is 
        /// not present in the BlockingCollection</returns>
        public static T Take<T>(this BlockingCollection<T> collection, T item)
        {
            T ret = default(T);
            bool foundItem = false; 
            List<T> removedItems = new List<T>();

            // Empty the collection
            for (int i = 0; i < collection.Count; i++)
            {
                T removed;
                if (collection.TryTake(out removed))
                {
                    removedItems.Add(removed);
                }
            }

            // Add them back to the collection minus the specified item
            foreach (T removedItem in removedItems)
            {
                if (removedItem.Equals(item))
                {
                    ret = removedItem;
                    foundItem = true;
                }
                else
                {
                    collection.Add(removedItem);
                }
            }

            if (!foundItem)
            {
                // Error: the element wasn't in the collection
                throw new InvalidOperationException(item + " not found in blocking collection");
            }

            return ret;
        }
    }
}
