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
    /// A proxy class for <see cref="FileInfo"/>.
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

        public static IFileInfo FromFileInfo(FileInfo fileInfo)
        {
            return new DefaultFileInfo(fileInfo);
        }

        public FileAttributes Attributes
        {
            get { return _fileInfo.Attributes; }
        }

        public DateTime CreationTime
        {
            get { return _fileInfo.CreationTime; }
        }

        public DateTime CreationTimeUtc
        {
            get { return _fileInfo.CreationTimeUtc; }
        }

        public IDirectoryInfo Directory
        {
            get { return DefaultDirectoryInfo.FromDirectoryInfo(_fileInfo.Directory); }
        }

        public string DirectoryName
        {
            get { return _fileInfo.DirectoryName; }
        }

        public bool Exists
        {
            get { return _fileInfo.Exists; }
        }

        public string Extension
        {
            get { return _fileInfo.Extension; }
        }

        public string FullName
        {
            get { return _fileInfo.FullName; }
        }

        public bool IsReadOnly
        {
            get { return _fileInfo.IsReadOnly; }
        }

        public DateTime LastAccessTime
        {
            get { return _fileInfo.LastAccessTime; }
        }

        public DateTime LastAccessTimeUtc
        {
            get { return _fileInfo.LastAccessTimeUtc; }
        }

        public DateTime LastWriteTime
        {
            get { return _fileInfo.LastWriteTime; }
        }

        public DateTime LastWriteTimeUtc
        {
            get { return _fileInfo.LastWriteTimeUtc; }
        }

        public long Length
        {
            get { return _fileInfo.Length; }
        }

        public string Name
        {
            get { return _fileInfo.Name; }
        }

        public StreamWriter AppendText()
        {
            return _fileInfo.AppendText();
        }

        public IFileInfo CopyTo(string destFileName)
        {
            return FromFileInfo(_fileInfo.CopyTo(destFileName));
        }

        public IFileInfo CopyTo(string destFileName, bool overwrite)
        {
            return FromFileInfo(_fileInfo.CopyTo(destFileName, overwrite));
        }

        public FileStream Create()
        {
            return _fileInfo.Create();
        }

        public StreamWriter CreateText()
        {
            return _fileInfo.CreateText();
        }

        public void Delete()
        {
            _fileInfo.Delete();
        }

        public void MoveTo(string destFileName)
        {
            _fileInfo.MoveTo(destFileName);
        }

        public FileStream Open(FileMode mode)
        {
            return _fileInfo.Open(mode);
        }

        public FileStream Open(FileMode mode, FileAccess access)
        {
            return _fileInfo.Open(mode, access);
        }

        public FileStream Open(FileMode mode, FileAccess access, FileShare share)
        {
            return _fileInfo.Open(mode, access, share);
        }

        public FileStream OpenRead()
        {
            return _fileInfo.OpenRead();
        }

        public StreamReader OpenText()
        {
            return _fileInfo.OpenText();
        }

        public FileStream OpenWrite()
        {
            return _fileInfo.OpenWrite();
        }

        public void Refresh()
        {
            _fileInfo.Refresh();
        }

        public IFileInfo Replace(string destinationFileName, string destinationBackupFileName)
        {
            return FromFileInfo(_fileInfo.Replace(destinationFileName, destinationBackupFileName));
        }

        public IFileInfo Replace(string destinationFileName, string destinationBackupFileName, bool ignoreMetadataErrors)
        {
            return FromFileInfo(_fileInfo.Replace(destinationFileName, destinationBackupFileName, ignoreMetadataErrors));
        }

        public void SetAccessControl(FileSecurity fileSecurity)
        {
            _fileInfo.SetAccessControl(fileSecurity);
        }

        public override bool Equals(object obj)
        {
            var other = obj as DefaultFileInfo;
            return other != null && Equals(other);
        }

        private bool Equals(DefaultFileInfo other)
        {
            return Equals(_fileInfo, other._fileInfo);
        }

        public override int GetHashCode()
        {
            return _fileInfo != null ? _fileInfo.GetHashCode() : 0;
        }

        public override string ToString()
        {
            return _fileInfo.ToString();
        }
    }
}