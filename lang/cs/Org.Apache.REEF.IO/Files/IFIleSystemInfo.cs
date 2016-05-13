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
using System.IO;

namespace Org.Apache.REEF.IO.Files
{
    /// <summary>
    /// This is meant only as a proxy interface for <see cref="FileSystemInfo"/> and has no 
    /// relation to classes in <see cref="Org.Apache.REEF.IO.FileSystem"/>.
    /// To create an <see cref="IFileSystemInfo"/> object from a <see cref="FileSystemInfo"/>
    /// object, please use the static factory method <see cref="FileSystemInfoFactory.FromFileSystemInfo"/>.
    /// </summary>
    public interface IFileSystemInfo
    {
        /// <summary>
        /// See <see cref="FileSystemInfo#Attributes"/>.
        /// </summary>
        FileAttributes Attributes { get; }

        /// <summary>
        /// See <see cref="FileSystemInfo#CreationTime"/>.
        /// </summary>
        DateTime CreationTime { get; }

        /// <summary>
        /// See <see cref="FileSystemInfo#CreationTimeUtc"/>.
        /// </summary>
        DateTime CreationTimeUtc { get; }

        /// <summary>
        /// See <see cref="FileSystemInfo#Exists"/>.
        /// </summary>
        bool Exists { get; }

        /// <summary>
        /// See <see cref="FileSystemInfo#Extension"/>.
        /// </summary>
        string Extension { get; }

        /// <summary>
        /// See <see cref="FileSystemInfo#FullName"/>.
        /// </summary>
        string FullName { get; }

        /// <summary>
        /// See <see cref="FileSystemInfo#LastAccessTime"/>.
        /// </summary>
        DateTime LastAccessTime { get; }

        /// <summary>
        /// See <see cref="FileSystemInfo#LastAccessTimeUtc"/>.
        /// </summary>
        DateTime LastAccessTimeUtc { get; }

        /// <summary>
        /// See <see cref="FileSystemInfo#LastWriteTime"/>.
        /// </summary>
        DateTime LastWriteTime { get; }

        /// <summary>
        /// See <see cref="FileSystemInfo#LastWriteTimeUtc"/>.
        /// </summary>
        DateTime LastWriteTimeUtc { get; }

        /// <summary>
        /// See <see cref="FileSystemInfo#Name"/>.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// See <see cref="FileSystemInfo#Delete"/>.
        /// </summary>
        void Delete();

        /// <summary>
        /// See <see cref="FileSystemInfo#Refresh"/>.
        /// </summary>
        void Refresh();
    }
}