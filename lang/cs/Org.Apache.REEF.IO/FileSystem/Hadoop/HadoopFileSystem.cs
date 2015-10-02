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
using System.Text.RegularExpressions;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IO.FileSystem.Hadoop
{
    /// <summary>
    /// Implements the IFileSystem interface for HDFS using external commands.
    /// </summary>
    /// <remarks>
    /// Note that operations with this class are enormously slow. If you can, use a more native way to access the file system
    /// in question.
    /// </remarks>
    /// <see cref="http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html" />
    internal sealed class HadoopFileSystem : IFileSystem
    {
        private static readonly Regex NoSuchFileOrDirectoryRegEx = new Regex("^ls: `.*': No such file or directory");
        private static readonly Regex LsFirstLineRegex = new Regex("^Found .* items");
        private readonly HdfsCommandRunner _commandRunner;

        [Inject]
        private HadoopFileSystem(HdfsCommandRunner commandRunner)
        {
            _commandRunner = commandRunner;
            UriPrefix = GetUriPrefix();
        }

        public string UriPrefix { get; private set; }

        /// <summary>
        /// Not implemented by this IFileSystem.
        /// </summary>
        /// <param name="fileUri"></param>
        /// <returns></returns>
        public Stream Open(Uri fileUri)
        {
            throw new NotImplementedException(
                "Open() is not supported by HadoopFileSystem. Use CopyToLocal and open the local file instead.");
        }

        /// <summary>
        /// Not implemented by this IFileSystem.
        /// </summary>
        /// <param name="fileUri"></param>
        /// <returns></returns>
        public Stream Create(Uri fileUri)
        {
            throw new NotImplementedException(
                "Create() is not supported by HadoopFileSystem. Create a local file and use CopyFromLocal instead.");
        }

        public void Delete(Uri fileUri)
        {
            // Delete the file via the hdfs command line.
            _commandRunner.Run("dfs -rm " + fileUri);
        }

        public bool Exists(Uri fileUri)
        {
            // This determines the existence of a file based on the 'ls' command. 
            // Ideally, we'd use the 'test' command's return value, but we did not find a way to access that.
            return
                _commandRunner.Run("dfs -ls " + fileUri).StdErr
                    .All(line => !NoSuchFileOrDirectoryRegEx.IsMatch(line));
        }

        public void Copy(Uri sourceUri, Uri destinationUri)
        {
            _commandRunner.Run("dfs -cp " + sourceUri + " " + destinationUri);
        }

        public void CopyToLocal(Uri remoteFileUri, string localName)
        {
            _commandRunner.Run("dfs -get " + remoteFileUri + " " + localName);
        }

        public void CopyFromLocal(string localFileName, Uri remoteFileUri)
        {
            _commandRunner.Run("dfs -put " + localFileName + " " + remoteFileUri);
        }

        public void CreateDirectory(Uri directoryUri)
        {
            _commandRunner.Run("dfs -mkdir " + directoryUri);
        }

        public void DeleteDirectory(Uri directoryUri)
        {
            _commandRunner.Run("dfs -rmdir " + directoryUri);
        }

        public IEnumerable<Uri> GetChildren(Uri directoryUri)
        {
            return _commandRunner.Run("dfs -ls " + directoryUri)
                .StdOut.Where(line => !LsFirstLineRegex.IsMatch(line))
                .Select(line => line.Split())
                .Select(x => new Uri(x[x.Length - 1]));
        }


        /// <summary>
        /// The Prefix used for URIs on this FileSystem.
        /// </summary>
        private string GetUriPrefix()
        {
            return _commandRunner.Run("getconf -confKey fs.defaultFS").StdOut.First();         
        }
    }
}