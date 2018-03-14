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
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IO.FileSystem.Local
{
    /// <summary>
    /// An implementation of IFileSystem on the filesystem of the host.
    /// </summary>
    internal sealed class LocalFileSystem : IFileSystem
    {
        [Inject]
        private LocalFileSystem()
        {
        }

        /// <summary>
        /// Create Uri from given file name
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public Uri CreateUriForPath(string path)
        {
            if (path == null)
            {
                throw new ArgumentException("null path passed in CreateUriForPath");
            }

            return new Uri(path);
        }

        /// <summary>
        /// Gets the FileStatus for given file.
        /// </summary>
        /// <param name="remoteFileUri"></param>
        /// <exception cref="ArgumentNullException">If file URI is null</exception>
        /// <returns>FileStatus</returns>
        public FileStatus GetFileStatus(Uri remoteFileUri)
        {
            if (remoteFileUri == null)
            {
                throw new ArgumentNullException("remoteFileUri");
            }

            FileInfo fileInfo = new FileInfo(remoteFileUri.LocalPath);
            return new FileStatus(fileInfo.LastWriteTime, fileInfo.Length);
        }

        public Stream Open(Uri fileUri)
        {
            return File.Open(fileUri.LocalPath, FileMode.Open);
        }

        public Stream Create(Uri fileUri)
        {
            return File.Open(fileUri.LocalPath, FileMode.Create);
        }

        public void Delete(Uri fileUri)
        {
            File.Delete(fileUri.LocalPath);
        }

        public bool Exists(Uri fileUri)
        {
            return File.Exists(fileUri.LocalPath);
        }

        public void Copy(Uri sourceUri, Uri destinationUri)
        {
            File.Copy(sourceUri.LocalPath, destinationUri.LocalPath);
        }

        public void CopyToLocal(Uri remoteFileUri, string localName)
        {
            EnsureDirectoryExists(localName);
            File.Copy(remoteFileUri.LocalPath, localName);
        }

        public void CopyFromLocal(string localFileName, Uri remoteFileUri)
        {
            EnsureDirectoryExists(remoteFileUri.LocalPath);
            File.Copy(localFileName, remoteFileUri.LocalPath);
        }

        public void CreateDirectory(Uri directoryUri)
        {
            Directory.CreateDirectory(directoryUri.LocalPath);
        }

        public void DeleteDirectory(Uri directoryUri)
        {
            Directory.Delete(directoryUri.LocalPath);
        }

        public IEnumerable<Uri> GetChildren(Uri directoryUri)
        {
            var localPath = Path.GetFullPath(directoryUri.LocalPath);
            try
            {
                return Directory.GetFileSystemEntries(localPath, "*", SearchOption.AllDirectories)
                    .Select(entry => new Uri(Path.Combine(localPath, entry)));
            }
            catch (DirectoryNotFoundException)
            {
                return new Collection<Uri>();
            }
        }

        private static void EnsureDirectoryExists(string filePath)
        {
            var directory = Path.GetDirectoryName(filePath);
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }
        }
    }
}