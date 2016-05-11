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
using System.Linq;
using System.Security.AccessControl;

namespace Org.Apache.REEF.IO.Files
{
    /// <summary>
    /// A proxy class for <see cref="DirectoryInfo"/>.
    /// To create a <see cref="DefaultDirectoryInfo"/> object from a <see cref="DirectoryInfo"/> object,
    /// please use the static factory method <see cref="FromDirectoryInfo"/>.
    /// </summary>
    public sealed class DefaultDirectoryInfo : IDirectoryInfo
    {
        private readonly DirectoryInfo _directoryInfo;

        private DefaultDirectoryInfo(DirectoryInfo info)
        {
            _directoryInfo = info;
        }

        public static IDirectoryInfo FromDirectoryInfo(DirectoryInfo info)
        {
            return new DefaultDirectoryInfo(info);
        }

        public FileAttributes Attributes
        {
            get { return _directoryInfo.Attributes; }
        }

        public DateTime CreationTime
        {
            get { return _directoryInfo.CreationTime; }
        }

        public DateTime CreationTimeUtc
        {
            get { return _directoryInfo.CreationTimeUtc; }
        }

        public bool Exists
        {
            get { return _directoryInfo.Exists; }
        }

        public string Extension
        {
            get { return _directoryInfo.Extension; }
        }

        public string FullName
        {
            get { return _directoryInfo.FullName; }
        }

        public DateTime LastAccessTime
        {
            get { return _directoryInfo.LastAccessTime; }
        }

        public DateTime LastAccessTimeUtc
        {
            get { return _directoryInfo.LastAccessTime; }
        }

        public DateTime LastWriteTime
        {
            get { return _directoryInfo.LastWriteTime; }
        }

        public DateTime LastWriteTimeUtc
        {
            get { return _directoryInfo.LastWriteTimeUtc; }
        }

        public string Name
        {
            get { return _directoryInfo.Name; }
        }

        public IDirectoryInfo Parent
        {
            get { return FromDirectoryInfo(_directoryInfo.Parent); }
        }

        public IDirectoryInfo Root
        {
            get { return FromDirectoryInfo(_directoryInfo.Root); }
        }

        public void Delete()
        {
            _directoryInfo.Delete();
        }

        public void Refresh()
        {
            _directoryInfo.Refresh();
        }

        public void Create()
        {
            _directoryInfo.Create();
        }

        public void Create(DirectorySecurity directorySecurity)
        {
            _directoryInfo.Create(directorySecurity);
        }

        public IDirectoryInfo CreateSubdirectory(string path)
        {
            return FromDirectoryInfo(_directoryInfo.CreateSubdirectory(path));
        }

        public IDirectoryInfo CreateSubdirectory(string path, DirectorySecurity directorySecurity)
        {
            return FromDirectoryInfo(_directoryInfo.CreateSubdirectory(path, directorySecurity));
        }

        public void Delete(bool recursive)
        {
            _directoryInfo.Delete(recursive);
        }

        public IEnumerable<IDirectoryInfo> EnumerateDirectories()
        {
            return _directoryInfo.EnumerateDirectories().Select(FromDirectoryInfo);
        }

        public IEnumerable<IDirectoryInfo> EnumerateDirectories(string searchPattern)
        {
            return _directoryInfo.EnumerateDirectories(searchPattern).Select(FromDirectoryInfo);
        }

        public IEnumerable<IDirectoryInfo> EnumerateDirectories(string searchPattern, SearchOption searchOption)
        {
            return _directoryInfo.EnumerateDirectories(searchPattern, searchOption).Select(FromDirectoryInfo);
        }

        public IEnumerable<IFileInfo> EnumerateFiles()
        {
            return _directoryInfo.EnumerateFiles().Select(DefaultFileInfo.FromFileInfo);
        }

        public IEnumerable<IFileInfo> EnumerateFiles(string searchPattern)
        {
            return _directoryInfo.EnumerateFiles(searchPattern).Select(DefaultFileInfo.FromFileInfo);
        }

        public IEnumerable<IFileInfo> EnumerateFiles(string searchPattern, SearchOption searchOption)
        {
            return _directoryInfo.EnumerateFiles(searchPattern, searchOption).Select(DefaultFileInfo.FromFileInfo);
        }

        public IEnumerable<IFileSystemInfo> EnumerateFileSystemInfos()
        {
            return ConvertFileSystemInfoEnumerable(_directoryInfo.EnumerateFileSystemInfos());
        }

        public IEnumerable<IFileSystemInfo> EnumerateFileSystemInfos(string searchPattern)
        {
            return ConvertFileSystemInfoEnumerable(_directoryInfo.EnumerateFileSystemInfos(searchPattern));
        }

        public IEnumerable<IFileSystemInfo> EnumerateFileSystemInfos(string searchPattern, SearchOption searchOption)
        {
            return ConvertFileSystemInfoEnumerable(_directoryInfo.EnumerateFileSystemInfos(searchPattern, searchOption));
        }

        private static IEnumerable<IFileSystemInfo> ConvertFileSystemInfoEnumerable(IEnumerable<FileSystemInfo> fileSystemInfos)
        {
            return fileSystemInfos.Select(FileSystemInfoFactory.FromFileSystemInfo).Where(fsInfo => fsInfo != null);
        }

        public DirectorySecurity GetAccessControl()
        {
            return _directoryInfo.GetAccessControl();
        }

        public DirectorySecurity GetAccessControl(AccessControlSections includeSections)
        {
            return _directoryInfo.GetAccessControl(includeSections);
        }

        public IDirectoryInfo[] GetDirectories()
        {
            return ConvertDirectoryArray(_directoryInfo.GetDirectories());
        }

        public IDirectoryInfo[] GetDirectories(string searchPattern)
        {
            return ConvertDirectoryArray(_directoryInfo.GetDirectories(searchPattern));
        }

        public IDirectoryInfo[] GetDirectories(string searchPattern, SearchOption searchOption)
        {
            return ConvertDirectoryArray(_directoryInfo.GetDirectories(searchPattern, searchOption));
        }

        private static IDirectoryInfo[] ConvertDirectoryArray(DirectoryInfo[] directoryInfos)
        {
            var arr = new IDirectoryInfo[directoryInfos.Length];

            for (var i = 0; i < directoryInfos.Length; i++)
            {
                arr[i] = FromDirectoryInfo(directoryInfos[i]);
            }

            return arr;
        }

        public IFileInfo[] GetFiles()
        {
            return ConvertFileArray(_directoryInfo.GetFiles());
        }

        public IFileInfo[] GetFiles(string searchPattern)
        {
            return ConvertFileArray(_directoryInfo.GetFiles(searchPattern));
        }

        public IFileInfo[] GetFiles(string searchPattern, SearchOption searchOption)
        {
            return ConvertFileArray(_directoryInfo.GetFiles(searchPattern, searchOption));
        }

        private static IFileInfo[] ConvertFileArray(FileInfo[] fileInfos)
        {
            var arr = new IFileInfo[fileInfos.Length];

            for (var i = 0; i < fileInfos.Length; i++)
            {
                arr[i] = DefaultFileInfo.FromFileInfo(fileInfos[i]);
            }

            return arr;
        }

        public IFileSystemInfo[] GetFileSystemInfos()
        {
            return ConvertFileSystemInfoArray(_directoryInfo.GetFileSystemInfos());
        }

        public IFileSystemInfo[] GetFileSystemInfos(string searchPattern)
        {
            return ConvertFileSystemInfoArray(_directoryInfo.GetFileSystemInfos(searchPattern));
        }

        public IFileSystemInfo[] GetFileSystemInfos(string searchPattern, SearchOption searchOption)
        {
            return ConvertFileSystemInfoArray(_directoryInfo.GetFileSystemInfos(searchPattern, searchOption));
        }

        private static IFileSystemInfo[] ConvertFileSystemInfoArray(IEnumerable<FileSystemInfo> fsInfos)
        {
            return ConvertFileSystemInfoEnumerable(fsInfos).ToArray();
        }

        public void MoveTo(string destDirName)
        {
            _directoryInfo.MoveTo(destDirName);
        }

        public void SetAccessControl(DirectorySecurity directorySecurity)
        {
            _directoryInfo.SetAccessControl(directorySecurity);
        }

        public override bool Equals(object obj)
        {
            var other = obj as DefaultDirectoryInfo;
            return other != null && Equals(other);
        }

        private bool Equals(DefaultDirectoryInfo other)
        {
            return Equals(_directoryInfo, other._directoryInfo);
        }

        public override int GetHashCode()
        {
            return _directoryInfo != null ? _directoryInfo.GetHashCode() : 0;
        }

        public override string ToString()
        {
            return _directoryInfo.ToString();
        }
    }
}