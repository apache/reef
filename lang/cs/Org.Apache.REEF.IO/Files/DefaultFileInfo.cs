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
using System.Security.AccessControl;

namespace Org.Apache.REEF.IO.Files
{
    /// <summary>
    /// This is meant only as a proxy class for <see cref="FileInfo"/> and has no 
    /// relation to classes in <see cref="Org.Apache.REEF.IO.FileSystem"/>.
    /// To create a <see cref="DefaultFileInfo"/> object from a <see cref="FileInfo"/> object,
    /// please use the static method <see cref="FromFileInfo"/>.
    /// </summary>
    public sealed class DefaultFileInfo : IFileInfo
    {
        private readonly FileInfo _fileInfo;

        private DefaultFileInfo(FileInfo info)
        {
            _fileInfo = info;
        }

        /// <summary>
        /// Factory method to create an <see cref="IFileInfo"/> object
        /// from a <see cref="FileInfo"/> object.
        /// </summary>
        public static IFileInfo FromFileInfo(FileInfo fileInfo)
        {
            return new DefaultFileInfo(fileInfo);
        }

        /// <summary>
        /// See <see cref="FileInfo#Attributes"/>.
        /// </summary>
        public FileAttributes Attributes
        {
            get { return _fileInfo.Attributes; }
        }

        /// <summary>
        /// See <see cref="FileInfo#CreationTime"/>.
        /// </summary>
        public DateTime CreationTime
        {
            get { return _fileInfo.CreationTime; }
        }

        /// <summary>
        /// See <see cref="FileInfo#CreationTimeUtc"/>.
        /// </summary>
        public DateTime CreationTimeUtc
        {
            get { return _fileInfo.CreationTimeUtc; }
        }

        /// <summary>
        /// See <see cref="FileInfo#Directory"/>.
        /// </summary>
        public IDirectoryInfo Directory
        {
            get { return DefaultDirectoryInfo.FromDirectoryInfo(_fileInfo.Directory); }
        }

        /// <summary>
        /// See <see cref="FileInfo#DirectoryName"/>.
        /// </summary>
        public string DirectoryName
        {
            get { return _fileInfo.DirectoryName; }
        }

        /// <summary>
        /// See <see cref="FileInfo#Exists"/>.
        /// </summary>
        public bool Exists
        {
            get { return _fileInfo.Exists; }
        }

        /// <summary>
        /// See <see cref="FileInfo#Extension"/>.
        /// </summary>
        public string Extension
        {
            get { return _fileInfo.Extension; }
        }

        /// <summary>
        /// See <see cref="FileInfo#FullName"/>.
        /// </summary>
        public string FullName
        {
            get { return _fileInfo.FullName; }
        }

        /// <summary>
        /// See <see cref="FileInfo#IsReadOnly"/>.
        /// </summary>
        public bool IsReadOnly
        {
            get { return _fileInfo.IsReadOnly; }
        }

        /// <summary>
        /// See <see cref="FileInfo#LastAccessTime"/>.
        /// </summary>
        public DateTime LastAccessTime
        {
            get { return _fileInfo.LastAccessTime; }
        }

        /// <summary>
        /// See <see cref="FileInfo#LastAccessTimeUtc"/>.
        /// </summary>
        public DateTime LastAccessTimeUtc
        {
            get { return _fileInfo.LastAccessTimeUtc; }
        }

        /// <summary>
        /// See <see cref="FileInfo#LastWriteTime"/>.
        /// </summary>
        public DateTime LastWriteTime
        {
            get { return _fileInfo.LastWriteTime; }
        }

        /// <summary>
        /// See <see cref="FileInfo#LastWriteTimeUtc"/>.
        /// </summary>
        public DateTime LastWriteTimeUtc
        {
            get { return _fileInfo.LastWriteTimeUtc; }
        }

        /// <summary>
        /// See <see cref="FileInfo#Length"/>.
        /// </summary>
        public long Length
        {
            get { return _fileInfo.Length; }
        }

        /// <summary>
        /// See <see cref="FileInfo#Name"/>.
        /// </summary>
        public string Name
        {
            get { return _fileInfo.Name; }
        }

        /// <summary>
        /// See <see cref="FileInfo#AppendText"/>.
        /// </summary>
        public StreamWriter AppendText()
        {
            return _fileInfo.AppendText();
        }

        /// <summary>
        /// See <see cref="FileInfo#CopyTo(string)"/>.
        /// </summary>
        public IFileInfo CopyTo(string destFileName)
        {
            return FromFileInfo(_fileInfo.CopyTo(destFileName));
        }

        /// <summary>
        /// See <see cref="FileInfo#CopyTo(string, bool)"/>.
        /// </summary>
        public IFileInfo CopyTo(string destFileName, bool overwrite)
        {
            return FromFileInfo(_fileInfo.CopyTo(destFileName, overwrite));
        }

        /// <summary>
        /// See <see cref="FileInfo#Create"/>.
        /// </summary>
        public FileStream Create()
        {
            return _fileInfo.Create();
        }

        /// <summary>
        /// See <see cref="FileInfo#CreateText"/>.
        /// </summary>
        public StreamWriter CreateText()
        {
            return _fileInfo.CreateText();
        }

        /// <summary>
        /// See <see cref="FileInfo#Delete"/>.
        /// </summary>
        public void Delete()
        {
            _fileInfo.Delete();
        }

        /// <summary>
        /// See <see cref="FileInfo#MoveTo(string)"/>.
        /// </summary>
        public void MoveTo(string destFileName)
        {
            _fileInfo.MoveTo(destFileName);
        }

        /// <summary>
        /// See <see cref="FileInfo#Open(FileMode)"/>.
        /// </summary>
        public FileStream Open(FileMode mode)
        {
            return _fileInfo.Open(mode);
        }

        /// <summary>
        /// See <see cref="FileInfo#Open(FileMode, FileAccess)"/>.
        /// </summary>
        public FileStream Open(FileMode mode, FileAccess access)
        {
            return _fileInfo.Open(mode, access);
        }

        /// <summary>
        /// See <see cref="FileInfo#Open(FileMode, FileAccess, FileShare)"/>.
        /// </summary>
        public FileStream Open(FileMode mode, FileAccess access, FileShare share)
        {
            return _fileInfo.Open(mode, access, share);
        }

        /// <summary>
        /// See <see cref="FileInfo#OpenRead"/>.
        /// </summary>
        public FileStream OpenRead()
        {
            return _fileInfo.OpenRead();
        }

        /// <summary>
        /// See <see cref="FileInfo#OpenText"/>.
        /// </summary>
        public StreamReader OpenText()
        {
            return _fileInfo.OpenText();
        }

        /// <summary>
        /// See <see cref="FileInfo#OpenWrite"/>.
        /// </summary>
        public FileStream OpenWrite()
        {
            return _fileInfo.OpenWrite();
        }

        /// <summary>
        /// See <see cref="FileInfo#Refresh"/>.
        /// </summary>
        public void Refresh()
        {
            _fileInfo.Refresh();
        }

        /// <summary>
        /// See <see cref="FileInfo#Replace(string, string)"/>.
        /// </summary>
        public IFileInfo Replace(string destinationFileName, string destinationBackupFileName)
        {
            return FromFileInfo(_fileInfo.Replace(destinationFileName, destinationBackupFileName));
        }

        /// <summary>
        /// See <see cref="FileInfo#Replace(string, string, bool)"/>.
        /// </summary>
        public IFileInfo Replace(string destinationFileName, string destinationBackupFileName, bool ignoreMetadataErrors)
        {
            return FromFileInfo(_fileInfo.Replace(destinationFileName, destinationBackupFileName, ignoreMetadataErrors));
        }

        /// <summary>
        /// See <see cref="FileInfo#SetAccessControl(FileSecurity)"/>.
        /// </summary>
        public void SetAccessControl(FileSecurity fileSecurity)
        {
            _fileInfo.SetAccessControl(fileSecurity);
        }

        /// <summary>
        /// See <see cref="FileInfo#Equals(object)"/>.
        /// </summary>
        public override bool Equals(object obj)
        {
            var other = obj as DefaultFileInfo;
            return other != null && Equals(other);
        }

        /// <summary>
        /// See <see cref="FileInfo#GetHashCode"/>.
        /// </summary>
        public override int GetHashCode()
        {
            return _fileInfo != null ? _fileInfo.GetHashCode() : 0;
        }

        /// <summary>
        /// See <see cref="FileInfo#ToString"/>.
        /// </summary>
        public override string ToString()
        {
            return _fileInfo.ToString();
        }

        private bool Equals(DefaultFileInfo other)
        {
            return Equals(_fileInfo, other._fileInfo);
        }
    }
}