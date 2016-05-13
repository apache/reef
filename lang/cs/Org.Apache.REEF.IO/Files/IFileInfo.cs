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

using System.IO;
using System.Security.AccessControl;

namespace Org.Apache.REEF.IO.Files
{
    /// <summary>
    /// This is meant only as a proxy interface for <see cref="FileInfo"/> and has no 
    /// relation to classes in <see cref="Org.Apache.REEF.IO.FileSystem"/>.
    /// To create an <see cref="IFileInfo"/> object from a <see cref="FileInfo"/> object,
    /// please use the static factory method <see cref="DefaultFileInfo.FromFileInfo"/>.
    /// </summary>
    public interface IFileInfo : IFileSystemInfo
    {
        /// <summary>
        /// See <see cref="FileInfo#Directory"/>.
        /// </summary>
        IDirectoryInfo Directory { get; }

        /// <summary>
        /// See <see cref="FileInfo#DirectoryName"/>.
        /// </summary>
        string DirectoryName { get; }

        /// <summary>
        /// See <see cref="FileInfo#IsReadOnly"/>.
        /// </summary>
        bool IsReadOnly { get; }

        /// <summary>
        /// See <see cref="FileInfo#Length"/>.
        /// </summary>
        long Length { get; }

        /// <summary>
        /// See <see cref="FileInfo#AppendText"/>.
        /// </summary>
        StreamWriter AppendText();

        /// <summary>
        /// See <see cref="FileInfo#CopyTo(string)"/>.
        /// </summary>
        IFileInfo CopyTo(string destFileName);

        /// <summary>
        /// See <see cref="FileInfo#CopyTo(string, bool)"/>.
        /// </summary>
        IFileInfo CopyTo(string destFileName, bool overwrite);

        /// <summary>
        /// See <see cref="FileInfo#Create"/>.
        /// </summary>
        FileStream Create();

        /// <summary>
        /// See <see cref="FileInfo#CreateText"/>.
        /// </summary>
        StreamWriter CreateText();

        /// <summary>
        /// See <see cref="FileInfo#MoveTo(string)"/>.
        /// </summary>
        void MoveTo(string destFileName);

        /// <summary>
        /// See <see cref="FileInfo#Open(FileMode)"/>.
        /// </summary>
        FileStream Open(FileMode mode);

        /// <summary>
        /// See <see cref="FileInfo#Open(FileMode, FileAccess)"/>.
        /// </summary>
        FileStream Open(FileMode mode, FileAccess access);

        /// <summary>
        /// See <see cref="FileInfo#(FileMode, FileAccess, FileShare)"/>.
        /// </summary>
        FileStream Open(FileMode mode, FileAccess access, FileShare share);

        /// <summary>
        /// See <see cref="FileInfo#OpenRead"/>.
        /// </summary>
        FileStream OpenRead();

        /// <summary>
        /// See <see cref="FileInfo#OpenText"/>.
        /// </summary>
        StreamReader OpenText();

        /// <summary>
        /// See <see cref="FileInfo#OpenWrite"/>.
        /// </summary>
        FileStream OpenWrite();

        /// <summary>
        /// See <see cref="FileInfo#Replace(string, string)"/>.
        /// </summary>
        IFileInfo Replace(string destinationFileName, string destinationBackupFileName);

        /// <summary>
        /// See <see cref="FileInfo#Replace(string, string, bool)"/>.
        /// </summary>
        IFileInfo Replace(string destinationFileName, string destinationBackupFileName, bool ignoreMetadataErrors);

        /// <summary>
        /// See <see cref="FileInfo#SetAccessControl(FileSecurity)"/>.
        /// </summary>
        void SetAccessControl(FileSecurity fileSecurity);
    }
}