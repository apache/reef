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
    /// A proxy interface for <see cref="FileInfo"/>.
    /// To create an <see cref="IFileInfo"/> object from a <see cref="FileInfo"/> object,
    /// please use the static factory method <see cref="DefaultFileInfo.FromFileInfo"/>.
    /// </summary>
    public interface IFileInfo : IFileSystemInfo
    {
        IDirectoryInfo Directory { get; }

        string DirectoryName { get; }

        bool IsReadOnly { get; }

        long Length { get; }

        StreamWriter AppendText();

        IFileInfo CopyTo(string destFileName);

        IFileInfo CopyTo(string destFileName, bool overwrite);

        FileStream Create();

        StreamWriter CreateText();

        void MoveTo(string destFileName);

        FileStream Open(FileMode mode);

        FileStream Open(FileMode mode, FileAccess access);

        FileStream Open(FileMode mode, FileAccess access, FileShare share);

        FileStream OpenRead();

        StreamReader OpenText();

        FileStream OpenWrite();

        IFileInfo Replace(string destinationFileName, string destinationBackupFileName);

        IFileInfo Replace(string destinationFileName, string destinationBackupFileName, bool ignoreMetadataErrors);

        void SetAccessControl(FileSecurity fileSecurity);
    }
}