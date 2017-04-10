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
using System.Text;
using System.Threading;
using Microsoft.VisualStudio.QualityTools.UnitTestFramework;
using Xunit;
using Org.Apache.REEF.Network.Utilities;

namespace Org.Apache.REEF.Network.Tests.NamingService
{
    public class GenericInMemoryCacheTests
    {
        [Fact]
        public void Cache_Global_Working_Ok()
        {
            // Create the global Cache and check the lazy instantiation
            Assert.NotNull(GenericInMemoryCache.Global);

            // Cache a new object for 1 second and test its expiry
            GenericInMemoryCache.Global.AddOrUpdate("test", new object(), 1);
            Assert.True(GenericInMemoryCache.Global.Exists("test"));

            Thread.Sleep(1050); // wait a bit more than a second

            Assert.False(GenericInMemoryCache.Global.Exists("test"));
        }

        [Fact]
        public void Cache_Update_Item_Ok()
        {
            GenericInMemoryCache c = new GenericInMemoryCache();

            object o1 = new object();
            object o2 = new object();

            c.AddOrUpdate("test", o1, 1);
            Assert.Same(c.Get("test"), o1);
            c.AddOrUpdate("test", o2, 1);
            Assert.Same(c.Get("test"), o2);

            Thread.Sleep(1050);
            Assert.False(c.Exists("test"));
        }

        [Fact]
        public void Cache_Generic_Ok()
        {
            GenericInMemoryCache<string, int> c = new GenericInMemoryCache<string, int>();

            c.AddOrUpdate("test", 42, 1);
            Assert.True(c.Exists("test"));
            Assert.Equal(c.Get("test"), 42);

            Thread.Sleep(1050);

            Assert.False(c.Exists("test"));
        }

        [Fact]
        public void Cache_Restart_Timer_Ok()
        {
            GenericInMemoryCache c = new GenericInMemoryCache();

            object o1 = new object();

            c.AddOrUpdate("test", o1, 1000);
            Thread.Sleep(800); // wait almost a second

            Assert.True(c.Exists("test")); // still exists

            c.AddOrUpdate("test", o1, 1, true); // update and refresh the timer
            Thread.Sleep(1000); // wait another second

            Assert.True(c.Exists("test")); // still exists

            c.AddOrUpdate("test", o1, 1, false); // default parameter 4: false - no refresh of the timer

            Thread.Sleep(500); // it should expire now

            Assert.Null(c.Get("test")); // no longer cached
        }

        [Fact]
        public void Cache_Indexer_Found_Ok()
        {
            GenericInMemoryCache c = new GenericInMemoryCache();

            object o1 = new object();

            c.AddOrUpdate("test", o1, 1);
            Assert.Same(c.Get("test"), o1);
        }

        [Fact]
        public void Cache_Indexer_Not_Found_Ok()
        {
            GenericInMemoryCache c = new GenericInMemoryCache();
            Assert.Same(c.Get("test2"), null);
        }

        [Fact]
        public void Cache_Clear_Ok()
        {
            GenericInMemoryCache c = new GenericInMemoryCache();
            c.AddOrUpdate("test", new object());
            Assert.True(c.Exists("test"));
            c.Clear();
            Assert.False(c.Exists("test"));
        }

        [Fact]
        public void Cache_Remove_By_Pattern_Ok()
        {
            GenericInMemoryCache c = new GenericInMemoryCache();
            c.AddOrUpdate("test1", new object());
            c.AddOrUpdate("test2", new object());
            c.AddOrUpdate("test3", new object());
            c.AddOrUpdate("Other", new object());
            Assert.True(c.Exists("test1"));
            Assert.True(c.Exists("Other"));

            c.Remove(k => k.StartsWith("test"));

            Assert.False(c.Exists("test1"));
            Assert.False(c.Exists("test2"));
            Assert.False(c.Exists("test3"));
            Assert.True(c.Exists("Other"));
        }
    }
}
