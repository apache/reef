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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IO.FileSystem.Hadoop
{
    /// <summary>
    /// Implements the IFileSystem interface for HDFS using external commands.
    /// </summary>
    /// <remarks>
    /// Note that operations with this class are enormously slow. If you can, use a more native way to access the file system
    /// in question.
    /// </remarks>
    /// <a href="http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html">
    /// FileSystemShell</a>
    internal sealed class HadoopFileSystem : IFileSystem
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(HadoopFileSystem));

        private static readonly Regex NoSuchFileOrDirectoryRegEx = new Regex("^ls: `.*': No such file or directory");
        private static readonly Regex LsFirstLineRegex = new Regex("^Found .* items");
        private readonly HdfsCommandRunner _commandRunner;
        private readonly string _uriPrefix;
        private const string PrefixTemplate = "{0}://{1}/";

        [Inject]
        private HadoopFileSystem(HdfsCommandRunner commandRunner)
        {
            _commandRunner = commandRunner;
            _uriPrefix = GetUriPrefix();
        }

        /// <summary>
        /// Create Uri from a given file name
        /// If the path already contains prefix, use it directly and verify the prefix after it is created.
        /// Otherwise add the prefix in fron of the relative path.
        /// If path is null or the prefix doesn't match the prefix in the FileSystem, throw ArgumentException
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public Uri CreateUriForPath(string path)
        {
            Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "CreateUriForPath with path: {0}, _uriPrefix: {1}.", path, _uriPrefix));
            if (path == null)
            {
                throw new ArgumentException("null path passed in CreateUriForPath");
            }

            Uri uri;
            try
            {
                uri = new Uri(path);
                Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "Uri {0} created in CreateUriForPath.", uri));
            }
            catch (UriFormatException)
            {
                uri = new Uri(_uriPrefix + path);
                Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "Uri {0} created in CreateUriForPath with prefix added.", uri));
            }

            return uri;
        }

        /// <summary>
        /// Not implemented by this IFileSystem.
        /// </summary>
        /// <param name="remoteFileUri"></param>
        /// <returns></returns>
        public FileStatus GetFileStatus(Uri remoteFileUri)
        {
            throw new NotImplementedException("GetFileStatus() is not implemented for HadoopFileSystem");
        }

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

        private string GetUriPrefix()
        {
            return _commandRunner.Run("getconf -confKey fs.default.name").StdOut.First();
        }
    }
}