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
using Org.Apache.REEF.IO.FileSystem.Local;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IO.FileSystem
{
    /// <summary>
    /// A file system abstraction.
    /// </summary>
    [DefaultImplementation(typeof(LocalFileSystem), "default")]
    public interface IFileSystem
    {
        /// <summary>
        /// Opens the given URI for reading
        /// </summary>
        /// <param name="fileUri"></param>
        /// <returns></returns>
        /// <exception cref="IOException">If the URI couldn't be opened.</exception>
        Stream Open(Uri fileUri);

        /// <summary>
        /// Creates a new file under the given URI.
        /// </summary>
        /// <param name="fileUri"></param>
        /// <returns></returns>
        /// <exception cref="IOException">If the URI couldn't be created.</exception>
        Stream Create(Uri fileUri);

        /// <summary>
        /// Deletes the file under the given URI.
        /// </summary>
        /// <param name="fileUri"></param>
        /// <exception cref="IOException"></exception>
        void Delete(Uri fileUri);

        /// <summary>
        /// Determines whether a file exists under the given URI.
        /// </summary>
        /// <param name="fileUri"></param>
        /// <returns></returns>
        bool Exists(Uri fileUri);

        /// <summary>
        /// Copies the file referenced  by sourceUri to destinationUri.
        /// </summary>
        /// <param name="sourceUri"></param>
        /// <param name="destinationUri"></param>
        /// <exception cref="IOException"></exception>
        void Copy(Uri sourceUri, Uri destinationUri);

        /// <summary>
        /// Copies the remote file to a local file.
        /// </summary>
        /// <param name="remoteFileUri"></param>
        /// <param name="localName"></param>
        /// <exception cref="IOException"></exception>
        void CopyToLocal(Uri remoteFileUri, string localName);

        /// <summary>
        /// Copies the specified file to the remote location.
        /// </summary>
        /// <param name="localFileName"></param>
        /// <param name="remoteFileUri"></param>
        /// <exception cref="IOException"></exception>
        void CopyFromLocal(string localFileName, Uri remoteFileUri);

        /// <summary>
        /// Creates a new directory.
        /// </summary>
        /// <param name="directoryUri"></param>
        /// <exception cref="IOException"></exception>
        void CreateDirectory(Uri directoryUri);

        /// <summary>
        /// Checks if uri is a directory uri.
        /// </summary>
        /// <param name="uri">uri of the directory/file</param>
        /// <returns>true if uri is for a directory else false</returns>
        bool IsDirectory(Uri uri);

        /// <summary>
        /// Deletes a directory.
        /// </summary>
        /// <param name="directoryUri"></param>
        /// <exception cref="IOException"></exception>
        void DeleteDirectory(Uri directoryUri);

        /// <summary>
        /// Get the children on the given URI, if that refers to a directory.
        /// </summary>
        /// <param name="directoryUri"></param>
        /// <returns></returns>
        /// <exception cref="IOException"></exception>
        IEnumerable<Uri> GetChildren(Uri directoryUri);

        /// <summary>
        /// Create Uri from a given file path.
        /// The file path can be full with prefix or relative without prefix.
        /// If null is passed as the path, ArgumentException will be thrown.
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        Uri CreateUriForPath(string path);

        /// <summary>
        /// Gets the FileStatus for remote file.
        /// </summary>
        /// <param name="remoteFileUri"></param>
        /// <exception cref="ArgumentNullException">If remote file URI is null</exception>
        /// <returns>FileStatus</returns>
        FileStatus GetFileStatus(Uri remoteFileUri);
    }
}