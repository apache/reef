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
using System.Collections.Concurrent;
using Org.Apache.REEF.Network.Utilities;
using Xunit;

namespace Org.Apache.REEF.Network.Tests
{
    public class BlockingCollectionExtensionTests
    {
        [Fact]
        public void TestCollectionContainsElement()
        {
            string item = "abc";
            BlockingCollection<string> collection = new BlockingCollection<string>();
            collection.Add(item);

            Assert.Equal(item, collection.Take(item));

            // Check that item is no longer in collection
            Assert.Equal(0, collection.Count);
        }

        [Fact]
        public void TestCollectionContainsElement2()
        {
            string item = "abc";
            BlockingCollection<string> collection = new BlockingCollection<string>();
            collection.Add("cat");
            collection.Add(item);
            collection.Add("dog");

            Assert.Equal(item, collection.Take(item));

            // Remove remaining items, check that item is not there
            Assert.NotEqual(item, collection.Take());
            Assert.NotEqual(item, collection.Take());
            Assert.Equal(0, collection.Count);
        }

        [Fact]
        public void TestCollectionDoesNotContainsElement()
        {
            string item1 = "abc";
            string item2 = "def";

            BlockingCollection<string> collection = new BlockingCollection<string>();
            collection.Add(item2);

            // Should throw InvalidOperationException since item1 is not in collection
            Assert.Throws<InvalidOperationException>(() => collection.Take(item1));
        }
    }
}
