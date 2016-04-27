﻿// Licensed to the Apache Software Foundation (ASF) under one
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
using System.IO;
using System.Linq;
using System.Text;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Org.Apache.REEF.IO.Files;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.IO.Tests
{
    /// <summary>
    /// Tests for <see cref="DataCache{T}"/>.
    /// </summary>
    public sealed class TestDataCache 
    {
        private const string AnyTestStr = "AnyTestString";

        private static readonly byte[] AnyTestStrBytes = Encoding.UTF8.GetBytes(AnyTestStr);
        private static readonly MemoryStream AnyTestStrStream = new MemoryStream(AnyTestStrBytes);
        private static readonly string[] MaterializeCompareArray = new[] { AnyTestStr };

        private TestContext _context;

        public TestDataCache()
        {
            _context = new TestContext();
        }

        /// <summary>
        /// Tests that caching to a lower level returns the lower level.
        /// </summary>
        [Fact]
        public void TestCacheToLowerLevelReturnsLowerLevel()
        {
            var cache = _context.GetDataCache();
            SubstituteForBasicDataMover();
            Assert.Equal(CacheLevel.Disk, cache.Cache(CacheLevel.Disk, false));
            Assert.Equal(CacheLevel.Disk, cache.CacheLevel);
            Assert.True(cache.DiskDirectory.IsPresent());
            Assert.Equal(CacheLevel.InMemoryAsStream, cache.Cache(CacheLevel.InMemoryAsStream, false));
            Assert.Equal(CacheLevel.InMemoryAsStream, cache.CacheLevel);
            Assert.True(cache.MemoryStreams.IsPresent());
            Assert.Equal(CacheLevel.InMemoryMaterialized, cache.Cache(CacheLevel.InMemoryMaterialized, false));
            Assert.Equal(CacheLevel.InMemoryMaterialized, cache.CacheLevel);
            Assert.True(cache.Materialized.IsPresent());
        }

        /// <summary>
        /// Tests that caching to a higher level does not perform any action.
        /// </summary>
        [Fact]
        public void TestCacheToHigherLevelReturnsLowerLevel()
        {
            var cache = _context.GetDataCache();
            SubstituteForBasicDataMover();
            Assert.Equal(CacheLevel.InMemoryMaterialized, cache.Cache(CacheLevel.InMemoryMaterialized, false));
            Assert.True(cache.Materialized.IsPresent());
            Assert.Equal(CacheLevel.InMemoryMaterialized, cache.Cache(CacheLevel.Disk, false));
            Assert.False(cache.DiskDirectory.IsPresent());
            Assert.Equal(CacheLevel.InMemoryMaterialized, cache.Cache(CacheLevel.InMemoryAsStream, false));
            Assert.False(cache.MemoryStreams.IsPresent());
            Assert.Equal(CacheLevel.InMemoryMaterialized, cache.Cache(CacheLevel.InMemoryMaterialized, false));
        }

        /// <summary>
        /// Tests that using the clean cache option returns the right level.
        /// </summary>
        [Fact]
        public void TestCleanCacheWhileLoweringLevelReturnsRightLevel()
        {
            var cache = _context.GetDataCache();
            SubstituteForBasicDataMover();
            Assert.Equal(CacheLevel.NotLocal, cache.CacheLevel);
            Assert.Equal(CacheLevel.Disk, cache.Cache(CacheLevel.Disk, true));
            Assert.Equal(CacheLevel.Disk, cache.CacheLevel);
            Assert.Equal(CacheLevel.InMemoryAsStream, cache.Cache(CacheLevel.InMemoryAsStream, true));
            Assert.Equal(CacheLevel.InMemoryAsStream, cache.CacheLevel);
            Assert.Equal(CacheLevel.InMemoryMaterialized, cache.Cache(CacheLevel.InMemoryMaterialized, true));
            Assert.Equal(CacheLevel.InMemoryMaterialized, cache.CacheLevel);
            Assert.Equal(CacheLevel.NotLocal, cache.CleanCacheAtLevel(CacheLevel.InMemoryMaterialized));
            Assert.Equal(CacheLevel.NotLocal, cache.CacheLevel);
        }

        /// <summary>
        /// Tests that cleaning the cache cleans all upper level caches.
        /// </summary>
        [Fact]
        public void TestCleanCacheClearsAllUpperCacheLevels()
        {
            var cache = _context.GetDataCache();
            SubstituteForBasicDataMover();
            Assert.Equal(CacheLevel.Disk, cache.Cache(CacheLevel.Disk, false));
            Assert.Equal(CacheLevel.InMemoryAsStream, cache.Cache(CacheLevel.InMemoryAsStream, false));
            Assert.Equal(CacheLevel.InMemoryMaterialized, cache.Cache(CacheLevel.InMemoryMaterialized, true));
            Assert.Equal(CacheLevel.NotLocal, cache.CleanCacheAtLevel(CacheLevel.InMemoryMaterialized));
        }

        /// <summary>
        /// Tests clear cache cleans all state and returns right level.
        /// </summary>
        [Fact]
        public void TestClearCache()
        {
            var cache = _context.GetDataCache();
            SubstituteForBasicDataMover();
            Assert.Equal(CacheLevel.Disk, cache.Cache(CacheLevel.Disk, false));
            Assert.Equal(CacheLevel.InMemoryAsStream, cache.Cache(CacheLevel.InMemoryAsStream, false));
            Assert.Equal(CacheLevel.InMemoryMaterialized, cache.Cache(CacheLevel.InMemoryMaterialized, false));
            cache.Clear();
            Assert.False(cache.DiskDirectory.IsPresent());
            Assert.False(cache.MemoryStreams.IsPresent());
            Assert.False(cache.Materialized.IsPresent());
            Assert.Equal(CacheLevel.NotLocal, cache.CacheLevel);
        }

        /// <summary>
        /// Tests cleaning the cache with non-consecutive cache levels.
        /// </summary>
        [Fact]
        public void TestCleanCacheJumpLevels()
        {
            var cache = _context.GetDataCache();
            SubstituteForBasicDataMover();
            Assert.Equal(CacheLevel.Disk, cache.Cache(CacheLevel.Disk, false));
            Assert.Equal(CacheLevel.InMemoryMaterialized, cache.Cache(CacheLevel.InMemoryMaterialized, false));
            Assert.Equal(CacheLevel.Disk, cache.CleanCacheAtLevel(CacheLevel.InMemoryMaterialized));
        }

        /// <summary>
        /// Tests cleaning the cache returns the right level.
        /// </summary>
        [Fact]
        public void TestCleanCacheReturnsRightLevel()
        {
            var cache = _context.GetDataCache();
            SubstituteForBasicDataMover();
            cache.Cache(CacheLevel.InMemoryMaterialized, false);
            
            Assert.Equal(CacheLevel.NotLocal, cache.CleanCacheAtLevel(CacheLevel.InMemoryMaterialized));

            cache.Cache(CacheLevel.Disk, false);
            cache.Cache(CacheLevel.InMemoryAsStream, false);
            cache.Cache(CacheLevel.InMemoryMaterialized, false);
            Assert.Equal(CacheLevel.InMemoryAsStream, cache.CleanCacheAtLevel(CacheLevel.InMemoryMaterialized));
            Assert.Equal(CacheLevel.Disk, cache.CleanCacheAtLevel(CacheLevel.InMemoryAsStream));
            Assert.Equal(CacheLevel.NotLocal, cache.CleanCacheAtLevel(CacheLevel.Disk));
        }

        /// <summary>
        /// Tests materialization from different cache levels.
        /// </summary>
        [Fact]
        public void TestMaterializeFromDifferentLevels()
        {
            var cache = _context.GetDataCache();
            SubstituteForBasicDataMover();

            // Test remote to materialized
            Assert.True(MaterializeCompareArray.SequenceEqual(cache.Materialize()));
            _context.DataMover.ReceivedWithAnyArgs(1).RemoteToMaterialized();
            Assert.Equal(CacheLevel.NotLocal, cache.CacheLevel);

            Assert.True(MaterializeCompareArray.SequenceEqual(cache.MaterializeAndCache(false)));
            _context.DataMover.ReceivedWithAnyArgs(2).RemoteToMaterialized();

            Assert.Equal(CacheLevel.InMemoryMaterialized, cache.CacheLevel);
            cache.Clear();

            // Test disk to materialized
            cache.Cache(CacheLevel.Disk, false);
            Assert.True(MaterializeCompareArray.SequenceEqual(cache.Materialize()));
            _context.DataMover.ReceivedWithAnyArgs(1).DiskToMaterialized(_context.DirectoryInfo);

            Assert.Equal(CacheLevel.Disk, cache.CacheLevel);
            Assert.True(MaterializeCompareArray.SequenceEqual(cache.MaterializeAndCache(false)));
            _context.DataMover.ReceivedWithAnyArgs(2).DiskToMaterialized(_context.DirectoryInfo);

            Assert.Equal(CacheLevel.InMemoryMaterialized, cache.CacheLevel);
            cache.Clear();

            // Test memStream to materialized
            cache.Cache(CacheLevel.InMemoryAsStream, false);
            Assert.True(MaterializeCompareArray.SequenceEqual(cache.Materialize()));
            _context.DataMover.ReceivedWithAnyArgs(1).MemoryToMaterialized(new[] { AnyTestStrStream });

            Assert.Equal(CacheLevel.InMemoryAsStream, cache.CacheLevel);
            Assert.True(MaterializeCompareArray.SequenceEqual(cache.MaterializeAndCache(false)));
            _context.DataMover.ReceivedWithAnyArgs(2).MemoryToMaterialized(new[] { AnyTestStrStream });

            Assert.Equal(CacheLevel.InMemoryMaterialized, cache.CacheLevel);
            cache.Clear();
        }

        /// <summary>
        /// Tests that after successfully caching state, data is not moved again.
        /// </summary>
        [Fact]
        public void TestCachedDoesNotInvokeDataMover()
        {
            var cache = _context.GetDataCache();
            SubstituteForBasicDataMover();
            cache.Cache(CacheLevel.InMemoryMaterialized, false);
            _context.DataMover.ClearReceivedCalls();
            var materialized = cache.Materialize();
            Assert.True(MaterializeCompareArray.SequenceEqual(materialized));
            _context.DataMover.ReceivedWithAnyArgs(0).RemoteToMaterialized();
            cache.Cache(CacheLevel.InMemoryMaterialized, false);
            _context.DataMover.ReceivedWithAnyArgs(0).RemoteToMaterialized();
        }

        /// <summary>
        /// Tests that throwing an Exception in Data Mover does not corrupt
        /// the state of the cache.
        /// </summary>
        [Fact]
        public void TestErrorThrowingDataMoverDoesNotCorruptState()
        {
            var cache = _context.GetDataCache();
            SubstituteForBasicDataMover();

            // Test remote to disk.
            _context.DataMover.RemoteToDisk().ThrowsForAnyArgs(new Exception());

            try
            {
                cache.Cache(CacheLevel.Disk, false);
            }
            catch (Exception)
            {
                // Empty to suppress exception.
            }

            _context.DataMover.ReceivedWithAnyArgs(1).RemoteToDisk();
            Assert.Equal(CacheLevel.NotLocal, cache.CacheLevel);
            Assert.False(cache.DiskDirectory.IsPresent());

            // Test remote to memory stream.
            _context = new TestContext();
            cache = _context.GetDataCache();
            SubstituteForBasicDataMover();

            _context.DataMover.RemoteToMaterialized().ThrowsForAnyArgs(new Exception());

            try
            {
                cache.Cache(CacheLevel.InMemoryMaterialized, false);
            }
            catch (Exception)
            {
                // Empty to suppress exception.
            }

            _context.DataMover.ReceivedWithAnyArgs(1).RemoteToMaterialized();
            Assert.Equal(CacheLevel.NotLocal, cache.CacheLevel);
            Assert.False(cache.Materialized.IsPresent());

            // Test remote to materialized.
            _context = new TestContext();
            cache = _context.GetDataCache();
            SubstituteForBasicDataMover();

            _context.DataMover.RemoteToMemory().ThrowsForAnyArgs(new Exception());

            try
            {
                cache.Cache(CacheLevel.InMemoryAsStream, false);
            }
            catch (Exception)
            {
                // Empty to suppress exception.
            }

            _context.DataMover.ReceivedWithAnyArgs(1).RemoteToMemory();
            Assert.Equal(CacheLevel.NotLocal, cache.CacheLevel);
            Assert.False(cache.MemoryStreams.IsPresent());

            // Test disk to memory streams.
            _context = new TestContext();
            cache = _context.GetDataCache();
            SubstituteForBasicDataMover();

            cache.Cache(CacheLevel.Disk, false);
            _context.DataMover.DiskToMemory(_context.DirectoryInfo).ThrowsForAnyArgs(new Exception());

            try
            {
                cache.Cache(CacheLevel.InMemoryAsStream, true);
            }
            catch (Exception)
            {
                // Empty to suppress exception.
            }

            _context.DataMover.ReceivedWithAnyArgs(1).DiskToMemory(_context.DirectoryInfo);
            Assert.Equal(CacheLevel.Disk, cache.CacheLevel);
            Assert.True(cache.DiskDirectory.IsPresent());
            Assert.False(cache.MemoryStreams.IsPresent());

            // Test disk to materialized
            _context = new TestContext();
            cache = _context.GetDataCache();
            SubstituteForBasicDataMover();

            cache.Cache(CacheLevel.Disk, false);
            _context.DataMover.DiskToMaterialized(_context.DirectoryInfo).ThrowsForAnyArgs(new Exception());

            try
            {
                cache.Cache(CacheLevel.InMemoryMaterialized, true);
            }
            catch (Exception)
            {
                // Empty to suppress exception.
            }

            _context.DataMover.ReceivedWithAnyArgs(1).DiskToMaterialized(_context.DirectoryInfo);
            Assert.Equal(CacheLevel.Disk, cache.CacheLevel);
            Assert.True(cache.DiskDirectory.IsPresent());
            Assert.False(cache.Materialized.IsPresent());

            // Test memory streams to materialized.
            _context = new TestContext();
            cache = _context.GetDataCache();
            SubstituteForBasicDataMover();

            cache.Cache(CacheLevel.InMemoryAsStream, false);
            _context.DataMover.MemoryToMaterialized(new[] { AnyTestStrStream }).ThrowsForAnyArgs(new Exception());

            try
            {
                cache.Cache(CacheLevel.InMemoryMaterialized, true);
            }
            catch (Exception)
            {
                // Empty to suppress exception.
            }

            _context.DataMover.ReceivedWithAnyArgs(1).MemoryToMaterialized(new[] { AnyTestStrStream });
            Assert.Equal(CacheLevel.InMemoryAsStream, cache.CacheLevel);
            Assert.True(cache.MemoryStreams.IsPresent());
            Assert.False(cache.Materialized.IsPresent());
        }

        private void SubstituteForBasicDataMover()
        {
            _context.DataMover.RemoteToDisk().ReturnsForAnyArgs(_context.DirectoryInfo);
            _context.DataMover.RemoteToMemory().ReturnsForAnyArgs(new[] { AnyTestStrStream });
            _context.DataMover.RemoteToMaterialized().ReturnsForAnyArgs(new[] { AnyTestStr });
            _context.DataMover.DiskToMaterialized(_context.DirectoryInfo).ReturnsForAnyArgs(new[] { AnyTestStr });
            _context.DataMover.DiskToMemory(_context.DirectoryInfo).ReturnsForAnyArgs(new[] { AnyTestStrStream });
            _context.DataMover.MemoryToMaterialized(new[] { AnyTestStrStream }).ReturnsForAnyArgs(new[] { AnyTestStr });
        }

        private class TestContext
        {
            public readonly IInputDataMover<string> DataMover = Substitute.For<IInputDataMover<string>>();

            public readonly IDirectoryInfo DirectoryInfo = Substitute.For<IDirectoryInfo>();

            public DataCache<string> GetDataCache()
            {
                var injector = TangFactory.GetTang().NewInjector();
                injector.BindVolatileInstance(GenericType<IInputDataMover<string>>.Class, DataMover);
                return injector.GetInstance<DataCache<string>>();
            }
        }
    }
}