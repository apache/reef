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

namespace Org.Apache.REEF.IO.Files
{
    /// <summary>
    /// A static factory class that creates an <see cref="IFileSystemInfo"/> object from
    /// a <see cref="FileSystemInfo"/> object.
    /// </summary>
    public static class FileSystemInfoFactory
    {
        public static IFileSystemInfo FromFileSystemInfo(FileSystemInfo fsInfo)
        {
            var dirInfo = fsInfo as DirectoryInfo;
            if (dirInfo != null)
            {
                return DefaultDirectoryInfo.FromDirectoryInfo(dirInfo);
            }

            var fileInfo = fsInfo as FileInfo;
            if (fileInfo != null)
            {
                return DefaultFileInfo.FromFileInfo(fileInfo);
            }

            return null;
        }
    }
}