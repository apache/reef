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
using System.IO;
using Org.Apache.REEF.IO.Files;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.IO
{
    /// <summary>
    /// A class that manages the caching of data at different layers of storage medium.
    /// </summary>
    [Unstable("0.15", "API contract may change.")]
    public sealed class DataCache<T>
    {
        private readonly IInputDataMover<T> _inputDataMover;

        private Optional<IDirectoryInfo> _diskDirectory = Optional<IDirectoryInfo>.Empty();
        private Optional<IReadOnlyCollection<MemoryStream>> _memStreams = Optional<IReadOnlyCollection<MemoryStream>>.Empty();
        private Optional<IReadOnlyCollection<T>> _materialized = Optional<IReadOnlyCollection<T>>.Empty();

        private CacheLevel _lowestCacheLevel = CacheLevel.NotLocal;

        [Inject]
        private DataCache(IInputDataMover<T> inputDataMover)
        {
            _inputDataMover = inputDataMover;
        }

        /// <summary>
        /// The current level at which the data is cached.
        /// </summary>
        public CacheLevel CacheLevel
        {
            get { return _lowestCacheLevel; }
        }

        /// <summary>
        /// The directory where cached data resides, if the data is cached on disk.
        /// </summary>
        public Optional<IDirectoryInfo> DiskDirectory
        {
            get { return _diskDirectory; }
        }

        /// <summary>
        /// The collection of memory streams where cached data resides, if the data is
        /// cached in memory.
        /// </summary>
        public Optional<IReadOnlyCollection<MemoryStream>> MemoryStreams
        {
            get { return _memStreams; }
        }

        /// <summary>
        /// The cached collection of "materialized" objects, if the data is cached
        /// in memory and deserialized.
        /// </summary>
        public Optional<IReadOnlyCollection<T>> Materialized
        {
            get { return _materialized; }
        }

        /// <summary>
        /// Clears all levels of cache.
        /// </summary>
        public void Clear()
        {
            while (CleanCacheAtLevel(CacheLevel) != CacheLevel.NotLocal)
            {
            }
        }

        /// <summary>
        /// Cache the data to a <see cref="CacheLevel"/>.
        /// If the data is already cached at a lower level, no action will be taken.
        /// </summary>
        /// <param name="cacheToLevel">Level to cache to.</param>
        /// <param name="shouldCleanHigherLevelCache">Whether or not to clean the higher level caches.</param>
        /// <returns>The cached level.</returns>
        public CacheLevel Cache(CacheLevel cacheToLevel, bool shouldCleanHigherLevelCache)
        {
            if (_lowestCacheLevel <= cacheToLevel)
            {
                return _lowestCacheLevel;
            }

            switch (cacheToLevel)
            {
                case CacheLevel.NotLocal:
                    return _lowestCacheLevel;
                case CacheLevel.Disk:
                    return CacheToDisk();
                case CacheLevel.InMemoryAsStream:
                    return CacheToMemory(shouldCleanHigherLevelCache);
                case CacheLevel.InMemoryMaterialized:
                    return CacheToMaterialized(shouldCleanHigherLevelCache);
                default:
                    throw new SystemException("Unexpected cache level " + cacheToLevel);
            }
        }

        /// <summary>
        /// Cleans up the cache at the specified cache level.
        /// </summary>
        /// <returns>the lowest cache level.</returns>
        public CacheLevel CleanCacheAtLevel(CacheLevel cacheLevel)
        {
            switch (cacheLevel)
            {
                case CacheLevel.Disk:
                    CleanUpDisk(false);
                    break;
                case CacheLevel.InMemoryAsStream:
                    CleanUpMemoryStreams(false);
                    break;
                case CacheLevel.InMemoryMaterialized:
                    CleanUpMaterialized(false);
                    break;
            }

            _lowestCacheLevel = FindLowestCacheLevel();
            return _lowestCacheLevel;
        }

        /// <summary>
        /// Finds the lowest cache level.
        /// </summary>
        private CacheLevel FindLowestCacheLevel()
        {
            if (CheckMaterialized(false))
            {
                return CacheLevel.InMemoryMaterialized;
            }

            if (CheckMemoryStreams(false))
            {
                return CacheLevel.InMemoryAsStream;
            }

            if (CheckDisk(false))
            {
                return CacheLevel.Disk;
            }

            return CacheLevel.NotLocal;
        }

        /// <summary>
        /// Cache to disk with the injected <see cref="IInputDataMover{T}"/>.
        /// </summary>
        /// <returns>The cached level.</returns>
        private CacheLevel CacheToDisk()
        {
            switch (_lowestCacheLevel)
            {
                case CacheLevel.NotLocal:
                    _diskDirectory = Optional<IDirectoryInfo>.Of(_inputDataMover.RemoteToDisk());
                    break;
                default:
                    throw new SystemException(
                        "Unexpected cache level transition from " + _lowestCacheLevel + " to " + CacheLevel.Disk);
            }

            _lowestCacheLevel = CacheLevel.Disk;
            return _lowestCacheLevel;
        }

        /// <summary>
        /// Cache to memory with the injected <see cref="IInputDataMover{T}"/>.
        /// </summary>
        /// <returns>The cached level.</returns>
        public CacheLevel CacheToMemory(bool shouldCleanHigherLevelCache)
        {
            Optional<IReadOnlyCollection<MemoryStream>> memStreams;
            switch (_lowestCacheLevel)
            {
                case CacheLevel.Disk:
                    CheckDisk(true);
                    memStreams = Optional<IReadOnlyCollection<MemoryStream>>.Of(
                        new List<MemoryStream>(_inputDataMover.DiskToMemory(_diskDirectory.Value)));
                    break;
                case CacheLevel.NotLocal:
                    memStreams = Optional<IReadOnlyCollection<MemoryStream>>.Of(
                        new List<MemoryStream>(_inputDataMover.RemoteToMemory()));
                    break;
                default:
                    throw new SystemException(
                        "Unexpected cache level transition from " + _lowestCacheLevel + " to " + CacheLevel.InMemoryAsStream);
            }

            if (shouldCleanHigherLevelCache)
            {
                CleanHigherLevelCache(CacheLevel.InMemoryAsStream);
            }

            _memStreams = memStreams;
            _lowestCacheLevel = CacheLevel.InMemoryAsStream;
            return _lowestCacheLevel;
        }

        /// <summary>
        /// "Materializes" and deserializes the data. Also caches to memory.
        /// To not cache to memory, please use the <see cref="Materialize"/> method.
        /// </summary>
        /// <returns>The deserialized data.</returns>
        public IEnumerable<T> MaterializeAndCache(bool shouldCleanHigherLevelCache)
        {
            return Materialize(true, shouldCleanHigherLevelCache);
        }

        /// <summary>
        /// "Materializes" and deserializes the data. Does not cache.
        /// To cache, please use the <see cref="MaterializeAndCache"/> method.
        /// </summary>
        /// <returns>The deserialized data.</returns>
        public IEnumerable<T> Materialize()
        {
            return Materialize(false, false);
        }

        private IEnumerable<T> Materialize(bool shouldCache, bool shouldCleanHigherLevelCache)
        {
            if (shouldCache)
            {
                CacheToMaterialized(shouldCleanHigherLevelCache);
            }

            switch (_lowestCacheLevel)
            {
                case CacheLevel.InMemoryAsStream:
                    CheckMemoryStreams(true);
                    return _inputDataMover.MemoryToMaterialized(_memStreams.Value);
                case CacheLevel.Disk:
                    CheckDisk(true);
                    return _inputDataMover.DiskToMaterialized(_diskDirectory.Value);
                case CacheLevel.NotLocal:
                    return _inputDataMover.RemoteToMaterialized();
                case CacheLevel.InMemoryMaterialized:
                    CheckMaterialized(true);
                    return _materialized.Value;
                default:
                    throw new IllegalStateException("Illegal cache layer.");
            }
        }

        /// <summary>
        /// Deserialize and cache the data in memory from the level the data is currently at.
        /// </summary>
        private CacheLevel CacheToMaterialized(bool shouldCleanHigherLevelCache)
        {
            Optional<IReadOnlyCollection<T>> materialized;

            switch (_lowestCacheLevel)
            {
                case CacheLevel.InMemoryAsStream:
                    CheckMemoryStreams(true);
                    materialized = Optional<IReadOnlyCollection<T>>.Of(
                        new List<T>(_inputDataMover.MemoryToMaterialized(_memStreams.Value)));
                    break;
                case CacheLevel.Disk:
                    CheckDisk(true);
                    materialized = Optional<IReadOnlyCollection<T>>.Of(
                        new List<T>(_inputDataMover.DiskToMaterialized(_diskDirectory.Value)));
                    
                    break;
                case CacheLevel.NotLocal:
                    materialized = Optional<IReadOnlyCollection<T>>.Of(
                        new List<T>(_inputDataMover.RemoteToMaterialized()));
                    break;
                default:
                    throw new SystemException(
                        "Unexpected cache level transition from " + _lowestCacheLevel + " to " + CacheLevel.InMemoryMaterialized);
            }

            if (shouldCleanHigherLevelCache)
            {
                CleanHigherLevelCache(CacheLevel.InMemoryMaterialized);
            }

            _materialized = materialized;
            _lowestCacheLevel = CacheLevel.InMemoryMaterialized;
            return _lowestCacheLevel;
        }

        /// <summary>
        /// Cleans up higher level cache recursively towards non-local.
        /// </summary>
        private void CleanHigherLevelCache(CacheLevel newLevel)
        {
            while (_lowestCacheLevel > newLevel)
            {
                switch (_lowestCacheLevel)
                {
                    case CacheLevel.InMemoryAsStream:
                        CleanUpMemoryStreams(true);
                        break;
                    case CacheLevel.Disk:
                        CleanUpDisk(true);
                        break;
                    case CacheLevel.InMemoryMaterialized:
                        CleanUpMaterialized(true);
                        break;
                    case CacheLevel.NotLocal:
                        // We cannot go to a higher level than this, return.
                        return;
                    default:
                        throw new SystemException("Unexpected cache level " + _lowestCacheLevel + ".");
                }

                var prevLowest = _lowestCacheLevel;
                _lowestCacheLevel = FindLowestCacheLevel();
                if (prevLowest > _lowestCacheLevel)
                {
                    throw new SystemException("The new lowest CacheLevel must be higher than the previous lowest CacheLevel.");
                }
            }
        }

        /// <summary>
        /// Check that the data is "materialized."
        /// </summary>
        private bool CheckMaterialized(bool shouldThrow)
        {
            if (_materialized.IsPresent())
            {
                return true;
            }

            if (shouldThrow)
            {
                throw new IllegalStateException("Collection is expected to be materialized in memory.");
            }

            return false;
        }

        /// <summary>
        /// Releases reference to the materialized collection such that
        /// garbage collection can collect. If the user holds a reference to the
        /// collection, this will not work.
        /// </summary>
        private void CleanUpMaterialized(bool shouldThrow)
        {
            if (CheckMaterialized(shouldThrow))
            {
                _materialized = Optional<IReadOnlyCollection<T>>.Empty();
            }
        }

        /// <summary>
        /// Check that the data is in memory as a <see cref="MemoryStream"/>.
        /// </summary>
        private bool CheckMemoryStreams(bool shouldThrow)
        {
            if (_memStreams.IsPresent())
            {
                return true;
            }

            if (shouldThrow)
            {
                throw new IllegalStateException("Data is expected to be in memory.");
            }

            return false;
        }

        /// <summary>
        /// Clean up the data stored as <see cref="MemoryStream"/>s.
        /// </summary>
        private void CleanUpMemoryStreams(bool shouldThrow)
        {
            if (CheckMemoryStreams(shouldThrow))
            {
                foreach (var memStream in _memStreams.Value)
                {
                    memStream.Dispose();
                }

                _memStreams = Optional<IReadOnlyCollection<MemoryStream>>.Empty();
            }
        }

        /// <summary>
        /// Check that the data is stored on disk.
        /// </summary>
        private bool CheckDisk(bool shouldThrow)
        {
            if (_diskDirectory.IsPresent())
            {
                return true;
            }

            if (shouldThrow)
            {
                throw new IllegalStateException("Disk directory is expected to be present.");
            }

            return false;
        }

        /// <summary>
        /// Clean up the data stored on disk.
        /// </summary>
        private void CleanUpDisk(bool shouldThrow)
        {
            if (CheckDisk(shouldThrow))
            {
                _diskDirectory.Value.Delete(true);
                _diskDirectory = Optional<IDirectoryInfo>.Empty();
            }
        }
    }
}