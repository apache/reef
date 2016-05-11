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

using System.Collections.Generic;
using System.IO;
using System.Security.AccessControl;

namespace Org.Apache.REEF.IO.Files
{
    /// <summary>
    /// This is meant only as a proxy interface for <see cref="DirectoryInfo"/> and has no 
    /// relation to classes in <see cref="Org.Apache.REEF.IO.FileSystem"/>.
    /// To create an <see cref="IDirectoryInfo"/> object from a <see cref="DirectoryInfo"/> object,
    /// please use the static method <see cref="DefaultDirectoryInfo.FromDirectoryInfo"/>.
    /// </summary>
    public interface IDirectoryInfo : IFileSystemInfo
    {
        /// <summary>
        /// See <see cref="DirectoryInfo#Parent"/>.
        /// </summary>
        IDirectoryInfo Parent { get; }

        /// <summary>
        /// See <see cref="DirectoryInfo#Root"/>.
        /// </summary>
        IDirectoryInfo Root { get; }

        /// <summary>
        /// See <see cref="DirectoryInfo#Create"/>.
        /// </summary>
        void Create();

        /// <summary>
        /// See <see cref="DirectoryInfo#Create(DirectorySecurity)"/>.
        /// </summary>
        void Create(DirectorySecurity directorySecurity);

        /// <summary>
        /// See <see cref="DirectoryInfo#CreateSubdirectory(string)"/>.
        /// </summary>
        IDirectoryInfo CreateSubdirectory(string path);

        /// <summary>
        /// See <see cref="DirectoryInfo#CreateSubdirectory(string, DirectorySecurity)"/>.
        /// </summary>
        IDirectoryInfo CreateSubdirectory(string path, DirectorySecurity directorySecurity);

        /// <summary>
        /// See <see cref="DirectoryInfo#Delete(bool)"/>.
        /// </summary>
        void Delete(bool recursive);

        /// <summary>
        /// See <see cref="DirectoryInfo#EnumerateDirectories"/>.
        /// </summary>
        IEnumerable<IDirectoryInfo> EnumerateDirectories();

        /// <summary>
        /// See <see cref="DirectoryInfo#EnumerateDirectories(string)"/>.
        /// </summary>
        IEnumerable<IDirectoryInfo> EnumerateDirectories(string searchPattern);

        /// <summary>
        /// See <see cref="DirectoryInfo#EnumerateDirectories(string, SearchOption)"/>.
        /// </summary>
        IEnumerable<IDirectoryInfo> EnumerateDirectories(string searchPattern, SearchOption searchOption);

        /// <summary>
        /// See <see cref="DirectoryInfo#EnumerateFiles"/>.
        /// </summary>
        IEnumerable<IFileInfo> EnumerateFiles();

        /// <summary>
        /// See <see cref="DirectoryInfo#EnumerateFiles(string)"/>.
        /// </summary>
        IEnumerable<IFileInfo> EnumerateFiles(string searchPattern);

        /// <summary>
        /// See <see cref="DirectoryInfo#EnumerateFiles(string, SearchOption)"/>.
        /// </summary>
        IEnumerable<IFileInfo> EnumerateFiles(string searchPattern, SearchOption searchOption);

        /// <summary>
        /// See <see cref="DirectoryInfo#EnumerateFileSystemInfos"/>.
        /// </summary>
        IEnumerable<IFileSystemInfo> EnumerateFileSystemInfos();

        /// <summary>
        /// See <see cref="DirectoryInfo#EnumerateFileSystemInfos(string)"/>.
        /// </summary>
        IEnumerable<IFileSystemInfo> EnumerateFileSystemInfos(string searchPattern);

        /// <summary>
        /// See <see cref="DirectoryInfo#EnumerateFileSystemInfos(string, SearchOption)"/>.
        /// </summary>
        IEnumerable<IFileSystemInfo> EnumerateFileSystemInfos(string searchPattern, SearchOption searchOption);

        /// <summary>
        /// See <see cref="DirectoryInfo#GetAccessControl"/>.
        /// </summary>
        DirectorySecurity GetAccessControl();

        /// <summary>
        /// See <see cref="DirectoryInfo#GetAccessControl(AccessControlSections)"/>.
        /// </summary>
        DirectorySecurity GetAccessControl(AccessControlSections includeSections);

        /// <summary>
        /// See <see cref="DirectoryInfo#GetDirectories"/>.
        /// </summary>
        IDirectoryInfo[] GetDirectories();

        /// <summary>
        /// See <see cref="DirectoryInfo#GetDirectories(string)"/>.
        /// </summary>
        IDirectoryInfo[] GetDirectories(string searchPattern);

        /// <summary>
        /// See <see cref="DirectoryInfo#GetDirectories(string, SearchOption)"/>.
        /// </summary>
        IDirectoryInfo[] GetDirectories(string searchPattern, SearchOption searchOption);

        /// <summary>
        /// See <see cref="DirectoryInfo#GetFiles"/>.
        /// </summary>
        IFileInfo[] GetFiles();

        /// <summary>
        /// See <see cref="DirectoryInfo#GetFiles(string)"/>.
        /// </summary>
        IFileInfo[] GetFiles(string searchPattern);

        /// <summary>
        /// See <see cref="DirectoryInfo#GetFiles(string, SearchOption)"/>.
        /// </summary>
        IFileInfo[] GetFiles(string searchPattern, SearchOption searchOption);

        /// <summary>
        /// See <see cref="DirectoryInfo#GetFileSystemInfos"/>.
        /// </summary>
        IFileSystemInfo[] GetFileSystemInfos();

        /// <summary>
        /// See <see cref="DirectoryInfo#GetFileSystemInfos(string)"/>.
        /// </summary>
        IFileSystemInfo[] GetFileSystemInfos(string searchPattern);

        /// <summary>
        /// See <see cref="DirectoryInfo#GetFileSystemInfos(string, SearchOption)"/>.
        /// </summary>
        IFileSystemInfo[] GetFileSystemInfos(string searchPattern, SearchOption searchOption);

        /// <summary>
        /// See <see cref="DirectoryInfo#MoveTo(string)"/>.
        /// </summary>
        void MoveTo(string destDirName);

        /// <summary>
        /// See <see cref="DirectoryInfo#SetAccessControl(DirectorySecurity)"/>.
        /// </summary>
        void SetAccessControl(DirectorySecurity directorySecurity);
    }
}