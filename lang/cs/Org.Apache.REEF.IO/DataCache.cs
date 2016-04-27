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
        private readonly IDataMover<T> _dataMover;

        private Optional<DirectoryInfo> _diskDirectory = Optional<DirectoryInfo>.Empty();
        private Optional<IReadOnlyCollection<MemoryStream>> _memStreams = Optional<IReadOnlyCollection<MemoryStream>>.Empty();
        private Optional<IReadOnlyCollection<T>> _materialized = Optional<IReadOnlyCollection<T>>.Empty();

        private CacheLevel _cacheLevel = CacheLevel.NotLocal;

        [Inject]
        private DataCache(IDataMover<T> dataMover)
        {
            _dataMover = dataMover;
        }

        /// <summary>
        /// The current level at which the data is cached.
        /// </summary>
        public CacheLevel CacheLevel
        {
            get { return _cacheLevel; }
        }

        /// <summary>
        /// The directory where cached data resides, if the data is cached on disk.
        /// </summary>
        public Optional<DirectoryInfo> DiskDirectory
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
        /// Cache the data to a <see cref="CacheLevel"/>.
        /// If the data is already cached at a lower level, no action will be taken.
        /// </summary>
        /// <param name="cacheToLevel">Level to cache to.</param>
        /// <returns>The cached level.</returns>
        public CacheLevel Cache(CacheLevel cacheToLevel)
        {
            if (_cacheLevel <= cacheToLevel)
            {
                return _cacheLevel;
            }

            switch (cacheToLevel)
            {
                case CacheLevel.NotLocal:
                    return _cacheLevel;
                case CacheLevel.Disk:
                    return CacheToDisk();
                case CacheLevel.InMemoryAsStream:
                    return CacheToMemory();
                case CacheLevel.InMemoryMaterialized:
                    return CacheToMaterialized();
                default:
                    throw new SystemException("Unexpected cache level " + cacheToLevel);
            }
        }

        /// <summary>
        /// Cache to disk with the injected <see cref="IDataMover{T}"/>.
        /// </summary>
        /// <returns>The cached level.</returns>
        private CacheLevel CacheToDisk()
        {
            switch (_cacheLevel)
            {
                case CacheLevel.NotLocal:
                    _diskDirectory = Optional<DirectoryInfo>.Of(_dataMover.RemoteToDisk());
                    break;
                default:
                    throw new SystemException(
                        "Unexpected cache level transition from " + _cacheLevel + " to " + CacheLevel.Disk);
            }

            _cacheLevel = CacheLevel.Disk;
            return _cacheLevel;
        }

        /// <summary>
        /// Cache to memory with the injected <see cref="IDataMover{T}"/>.
        /// </summary>
        /// <returns>The cached level.</returns>
        public CacheLevel CacheToMemory()
        {
            switch (_cacheLevel)
            {
                case CacheLevel.Disk:
                    CheckDisk();
                    _memStreams = Optional<IReadOnlyCollection<MemoryStream>>.Of(
                        new List<MemoryStream>(_dataMover.DiskToMemory(_diskDirectory.Value)));
                    CleanUpDisk();
                    break;
                case CacheLevel.NotLocal:
                    _memStreams = Optional<IReadOnlyCollection<MemoryStream>>.Of(
                        new List<MemoryStream>(_dataMover.RemoteToMemory()));
                    break;
                default:
                    throw new SystemException(
                        "Unexpected cache level transition from " + _cacheLevel + " to " + CacheLevel.InMemoryAsStream);
            }

            _cacheLevel = CacheLevel.InMemoryAsStream;
            return _cacheLevel;
        }

        /// <summary>
        /// "Materializes" and deserializes the data, with an option to cache.
        /// </summary>
        /// <returns>The deserialized data.</returns>
        public IEnumerable<T> Materialize(bool shouldCache)
        {
            if (shouldCache)
            {
                CacheToMaterialized();
            }

            switch (_cacheLevel)
            {
                case CacheLevel.InMemoryAsStream:
                    CheckMemory();
                    return _dataMover.MemoryToMaterialized(_memStreams.Value);
                case CacheLevel.Disk:
                    CheckDisk();
                    return _dataMover.DiskToMaterialized(_diskDirectory.Value);
                case CacheLevel.NotLocal:
                    return _dataMover.RemoteToMaterialized();
                case CacheLevel.InMemoryMaterialized:
                    CheckMaterialized();
                    return _materialized.Value;
                default:
                    throw new IllegalStateException("Illegal cache layer.");
            }
        }

        /// <summary>
        /// Deserialize and cache the data in memory from the level the data is currently at.
        /// </summary>
        private CacheLevel CacheToMaterialized()
        {
            switch (_cacheLevel)
            {
                case CacheLevel.InMemoryAsStream:
                    CheckMemory();
                    _materialized = Optional<IReadOnlyCollection<T>>.Of(
                        new List<T>(_dataMover.MemoryToMaterialized(_memStreams.Value)));
                    CleanUpMemory();
                    break;
                case CacheLevel.Disk:
                    CheckDisk();
                    _materialized = Optional<IReadOnlyCollection<T>>.Of(
                        new List<T>(_dataMover.DiskToMaterialized(_diskDirectory.Value)));
                    CleanUpDisk();
                    break;
                case CacheLevel.NotLocal:
                    _materialized = Optional<IReadOnlyCollection<T>>.Of(
                        new List<T>(_dataMover.RemoteToMaterialized()));
                    break;
                default:
                    throw new SystemException(
                        "Unexpected cache level transition from " + _cacheLevel + " to " + CacheLevel.InMemoryMaterialized);
            }

            _cacheLevel = CacheLevel.InMemoryMaterialized;
            return _cacheLevel;
        }

        /// <summary>
        /// Check that the data is "materialized."
        /// </summary>
        private void CheckMaterialized()
        {
            if (!_materialized.IsPresent())
            {
                throw new IllegalStateException("Collection is expected to be materialized in memory.");
            }
        }

        /// <summary>
        /// Check that the data is in memory as a <see cref="MemoryStream"/>.
        /// </summary>
        private void CheckMemory()
        {
            if (!_memStreams.IsPresent())
            {
                throw new IllegalStateException("Data is expected to be in memory.");
            }
        }

        /// <summary>
        /// Clean up the data stored as <see cref="MemoryStream"/>s.
        /// </summary>
        private void CleanUpMemory()
        {
            CheckMemory();

            foreach (var memStream in _memStreams.Value)
            {
                memStream.Dispose();
            }

            _memStreams = Optional<IReadOnlyCollection<MemoryStream>>.Empty();
        }

        /// <summary>
        /// Check that the data is stored on disk.
        /// </summary>
        private void CheckDisk()
        {
            if (!_diskDirectory.IsPresent())
            {
                throw new IllegalStateException("Disk directory is expected to be present.");
            }
        }

        /// <summary>
        /// Clean up the data stored on disk.
        /// </summary>
        private void CleanUpDisk()
        {
            CheckDisk();
            _diskDirectory.Value.Delete(true);
            _diskDirectory = Optional<DirectoryInfo>.Empty();
        }
    }
}