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
    /// This is meant only as a proxy class for <see cref="DirectoryInfo"/> and has no 
    /// relation to classes in <see cref="Org.Apache.REEF.IO.FileSystem"/>.
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

        /// <summary>
        /// Factory method to create an <see cref="IDirectoryInfo"/> object
        /// from a <see cref="DirectoryInfo"/> object.
        /// </summary>
        public static IDirectoryInfo FromDirectoryInfo(DirectoryInfo info)
        {
            return new DefaultDirectoryInfo(info);
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#Attributes"/>.
        /// </summary>
        public FileAttributes Attributes
        {
            get { return _directoryInfo.Attributes; }
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#CreationTime"/>.
        /// </summary>
        public DateTime CreationTime
        {
            get { return _directoryInfo.CreationTime; }
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#CreationTimeUtc"/>.
        /// </summary>
        public DateTime CreationTimeUtc
        {
            get { return _directoryInfo.CreationTimeUtc; }
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#Exists"/>.
        /// </summary>
        public bool Exists
        {
            get { return _directoryInfo.Exists; }
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#Extension"/>.
        /// </summary>
        public string Extension
        {
            get { return _directoryInfo.Extension; }
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#FullName"/>.
        /// </summary>
        public string FullName
        {
            get { return _directoryInfo.FullName; }
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#LastAccessTime"/>.
        /// </summary>
        public DateTime LastAccessTime
        {
            get { return _directoryInfo.LastAccessTime; }
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#LastAccessTimeUtc"/>.
        /// </summary>
        public DateTime LastAccessTimeUtc
        {
            get { return _directoryInfo.LastAccessTimeUtc; }
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#LastWriteTime"/>.
        /// </summary>
        public DateTime LastWriteTime
        {
            get { return _directoryInfo.LastWriteTime; }
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#LastWriteTimeUtc"/>.
        /// </summary>
        public DateTime LastWriteTimeUtc
        {
            get { return _directoryInfo.LastWriteTimeUtc; }
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#Name"/>.
        /// </summary>
        public string Name
        {
            get { return _directoryInfo.Name; }
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#Parent"/>.
        /// </summary>
        public IDirectoryInfo Parent
        {
            get { return FromDirectoryInfo(_directoryInfo.Parent); }
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#Root"/>.
        /// </summary>
        public IDirectoryInfo Root
        {
            get { return FromDirectoryInfo(_directoryInfo.Root); }
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#Delete"/>.
        /// </summary>
        public void Delete()
        {
            _directoryInfo.Delete();
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#Refresh"/>.
        /// </summary>
        public void Refresh()
        {
            _directoryInfo.Refresh();
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#Create"/>.
        /// </summary>
        public void Create()
        {
            _directoryInfo.Create();
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#Create(DirectorySecurity)"/>.
        /// </summary>
        public void Create(DirectorySecurity directorySecurity)
        {
            _directoryInfo.Create(directorySecurity);
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#CreateSubdirectory(string)"/>.
        /// </summary>
        public IDirectoryInfo CreateSubdirectory(string path)
        {
            return FromDirectoryInfo(_directoryInfo.CreateSubdirectory(path));
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#CreateSubdirectory(string, DirectorySecurity)"/>.
        /// </summary>
        public IDirectoryInfo CreateSubdirectory(string path, DirectorySecurity directorySecurity)
        {
            return FromDirectoryInfo(_directoryInfo.CreateSubdirectory(path, directorySecurity));
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#Delete(bool)"/>.
        /// </summary>
        public void Delete(bool recursive)
        {
            _directoryInfo.Delete(recursive);
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#EnumerateDirectories"/>.
        /// </summary>
        public IEnumerable<IDirectoryInfo> EnumerateDirectories()
        {
            return _directoryInfo.EnumerateDirectories().Select(FromDirectoryInfo);
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#EnumerateDirectories(string)"/>.
        /// </summary>
        public IEnumerable<IDirectoryInfo> EnumerateDirectories(string searchPattern)
        {
            return _directoryInfo.EnumerateDirectories(searchPattern).Select(FromDirectoryInfo);
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#EnumerateDirectories(string, SearchOption)"/>.
        /// </summary>
        public IEnumerable<IDirectoryInfo> EnumerateDirectories(string searchPattern, SearchOption searchOption)
        {
            return _directoryInfo.EnumerateDirectories(searchPattern, searchOption).Select(FromDirectoryInfo);
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#EnumerateFiles"/>.
        /// </summary>
        public IEnumerable<IFileInfo> EnumerateFiles()
        {
            return _directoryInfo.EnumerateFiles().Select(DefaultFileInfo.FromFileInfo);
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#EnumerateFiles(string)"/>.
        /// </summary>
        public IEnumerable<IFileInfo> EnumerateFiles(string searchPattern)
        {
            return _directoryInfo.EnumerateFiles(searchPattern).Select(DefaultFileInfo.FromFileInfo);
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#EnumerateFiles(string, SearchOption)"/>.
        /// </summary>
        public IEnumerable<IFileInfo> EnumerateFiles(string searchPattern, SearchOption searchOption)
        {
            return _directoryInfo.EnumerateFiles(searchPattern, searchOption).Select(DefaultFileInfo.FromFileInfo);
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#EnumerateFileSystemInfos"/>.
        /// </summary>
        public IEnumerable<IFileSystemInfo> EnumerateFileSystemInfos()
        {
            return ConvertFileSystemInfoEnumerable(_directoryInfo.EnumerateFileSystemInfos());
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#EnumerateFileSystemInfos(string)"/>.
        /// </summary>
        public IEnumerable<IFileSystemInfo> EnumerateFileSystemInfos(string searchPattern)
        {
            return ConvertFileSystemInfoEnumerable(_directoryInfo.EnumerateFileSystemInfos(searchPattern));
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#EnumerateFileSystemInfos(string, SearchOption)"/>.
        /// </summary>
        public IEnumerable<IFileSystemInfo> EnumerateFileSystemInfos(string searchPattern, SearchOption searchOption)
        {
            return ConvertFileSystemInfoEnumerable(_directoryInfo.EnumerateFileSystemInfos(searchPattern, searchOption));
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#GetAccessControl"/>.
        /// </summary>
        public DirectorySecurity GetAccessControl()
        {
            return _directoryInfo.GetAccessControl();
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#GetAccessControl(AccessControlSections)"/>.
        /// </summary>
        public DirectorySecurity GetAccessControl(AccessControlSections includeSections)
        {
            return _directoryInfo.GetAccessControl(includeSections);
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#GetDirectories"/>.
        /// </summary>
        public IDirectoryInfo[] GetDirectories()
        {
            return ConvertDirectoryArray(_directoryInfo.GetDirectories());
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#GetDirectories(string)"/>.
        /// </summary>
        public IDirectoryInfo[] GetDirectories(string searchPattern)
        {
            return ConvertDirectoryArray(_directoryInfo.GetDirectories(searchPattern));
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#GetDirectories(string, SearchOption)"/>.
        /// </summary>
        public IDirectoryInfo[] GetDirectories(string searchPattern, SearchOption searchOption)
        {
            return ConvertDirectoryArray(_directoryInfo.GetDirectories(searchPattern, searchOption));
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#GetFiles"/>.
        /// </summary>
        public IFileInfo[] GetFiles()
        {
            return ConvertFileArray(_directoryInfo.GetFiles());
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#GetFiles(string)"/>.
        /// </summary>
        public IFileInfo[] GetFiles(string searchPattern)
        {
            return ConvertFileArray(_directoryInfo.GetFiles(searchPattern));
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#GetFiles(string, SearchOption)"/>.
        /// </summary>
        public IFileInfo[] GetFiles(string searchPattern, SearchOption searchOption)
        {
            return ConvertFileArray(_directoryInfo.GetFiles(searchPattern, searchOption));
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#GetFileSystemInfos"/>.
        /// </summary>
        public IFileSystemInfo[] GetFileSystemInfos()
        {
            return ConvertFileSystemInfoArray(_directoryInfo.GetFileSystemInfos());
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#GetFileSystemInfos(string)"/>.
        /// </summary>
        public IFileSystemInfo[] GetFileSystemInfos(string searchPattern)
        {
            return ConvertFileSystemInfoArray(_directoryInfo.GetFileSystemInfos(searchPattern));
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#GetFileSystemInfos(string, SearchOption)"/>.
        /// </summary>
        public IFileSystemInfo[] GetFileSystemInfos(string searchPattern, SearchOption searchOption)
        {
            return ConvertFileSystemInfoArray(_directoryInfo.GetFileSystemInfos(searchPattern, searchOption));
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#MoveTo(string)"/>.
        /// </summary>
        public void MoveTo(string destDirName)
        {
            _directoryInfo.MoveTo(destDirName);
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#SetAccessControl(DirectorySecurity)"/>.
        /// </summary>
        public void SetAccessControl(DirectorySecurity directorySecurity)
        {
            _directoryInfo.SetAccessControl(directorySecurity);
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#Equals(object)"/>.
        /// </summary>
        public override bool Equals(object obj)
        {
            var other = obj as DefaultDirectoryInfo;
            return other != null && Equals(other);
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#GetHashCode"/>.
        /// </summary>
        public override int GetHashCode()
        {
            return _directoryInfo != null ? _directoryInfo.GetHashCode() : 0;
        }

        /// <summary>
        /// See <see cref="DirectoryInfo#ToString"/>.
        /// </summary>
        public override string ToString()
        {
            return _directoryInfo.ToString();
        }

        private static IFileSystemInfo[] ConvertFileSystemInfoArray(IEnumerable<FileSystemInfo> fsInfos)
        {
            return ConvertFileSystemInfoEnumerable(fsInfos).ToArray();
        }

        private static IEnumerable<IFileSystemInfo> ConvertFileSystemInfoEnumerable(IEnumerable<FileSystemInfo> fileSystemInfos)
        {
            return fileSystemInfos.Select(FileSystemInfoFactory.FromFileSystemInfo).Where(fsInfo => fsInfo != null);
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

        private static IFileInfo[] ConvertFileArray(FileInfo[] fileInfos)
        {
            var arr = new IFileInfo[fileInfos.Length];

            for (var i = 0; i < fileInfos.Length; i++)
            {
                arr[i] = DefaultFileInfo.FromFileInfo(fileInfos[i]);
            }

            return arr;
        }

        private bool Equals(DefaultDirectoryInfo other)
        {
            return Equals(_directoryInfo, other._directoryInfo);
        }
    }
}