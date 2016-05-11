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
    /// A proxy interface for <see cref="DirectoryInfo"/>.
    /// To create an <see cref="IDirectoryInfo"/> object from a <see cref="DirectoryInfo"/> object,
    /// please use the static method <see cref="DefaultDirectoryInfo.FromDirectoryInfo"/>.
    /// </summary>
    public interface IDirectoryInfo : IFileSystemInfo
    {
        IDirectoryInfo Parent { get; }

        IDirectoryInfo Root { get; }

        void Create();

        void Create(DirectorySecurity directorySecurity);

        IDirectoryInfo CreateSubdirectory(string path);

        IDirectoryInfo CreateSubdirectory(string path, DirectorySecurity directorySecurity);

        void Delete(bool recursive);

        IEnumerable<IDirectoryInfo> EnumerateDirectories();

        IEnumerable<IDirectoryInfo> EnumerateDirectories(string searchPattern);

        IEnumerable<IDirectoryInfo> EnumerateDirectories(string searchPattern, SearchOption searchOption);

        IEnumerable<IFileInfo> EnumerateFiles();

        IEnumerable<IFileInfo> EnumerateFiles(string searchPattern);

        IEnumerable<IFileInfo> EnumerateFiles(string searchPattern, SearchOption searchOption);

        IEnumerable<IFileSystemInfo> EnumerateFileSystemInfos();

        IEnumerable<IFileSystemInfo> EnumerateFileSystemInfos(string searchPattern);

        IEnumerable<IFileSystemInfo> EnumerateFileSystemInfos(string searchPattern, SearchOption searchOption);

        DirectorySecurity GetAccessControl();

        DirectorySecurity GetAccessControl(AccessControlSections includeSections);

        IDirectoryInfo[] GetDirectories();

        IDirectoryInfo[] GetDirectories(string searchPattern);

        IDirectoryInfo[] GetDirectories(string searchPattern, SearchOption searchOption);

        IFileInfo[] GetFiles();

        IFileInfo[] GetFiles(string searchPattern);

        IFileInfo[] GetFiles(string searchPattern, SearchOption searchOption);

        IFileSystemInfo[] GetFileSystemInfos();

        IFileSystemInfo[] GetFileSystemInfos(string searchPattern);

        IFileSystemInfo[] GetFileSystemInfos(string searchPattern, SearchOption searchOption);

        void MoveTo(string destDirName);

        void SetAccessControl(DirectorySecurity directorySecurity);
    }
}