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

using System.Collections.Generic;
using System.IO;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.Common
{
    /// <summary>
    /// Manages the files to be copied to driver folder.
    /// </summary>
    internal sealed class FileSets
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(FileSets));
        private readonly REEFFileNames _fileNames;

        private readonly ISet<string> _globalFileSet =
            new HashSet<string>(CaseIgnoringStringEqualityComparer.TheInstance);

        private readonly ISet<string> _localFileSet = new HashSet<string>(CaseIgnoringStringEqualityComparer.TheInstance);

        [Inject]
        internal FileSets(REEFFileNames fileNames)
        {
            _fileNames = fileNames;
        }

        /// <summary>
        /// Adds a file to be added to the driver's local folder.
        /// </summary>
        /// <remarks>Empty strings are quietly ignored.</remarks>
        /// <param name="path"></param>
        internal void AddToLocalFiles(string path)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                Logger.Log(Level.Warning, "Skipping: " + path);
            }
            else
            {
                _localFileSet.Add(Path.GetFullPath(path));
            }
        }

        /// <summary>
        /// Adds all the referenced files to the driver's local folder.
        /// </summary>
        /// <remarks>Empty strings are quietly ignored.</remarks>
        /// <param name="paths"></param>
        internal void AddToLocalFiles(IEnumerable<string> paths)
        {
            foreach (var path in paths)
            {
                AddToLocalFiles(path);
            }
        }

        /// <summary>
        /// Adds a file to be added to the driver's global folder.
        /// </summary>
        /// <remarks>Empty strings are quietly ignored.</remarks>
        /// <param name="path"></param>
        internal void AddToGlobalFiles(string path)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                Logger.Log(Level.Warning, "Skipping: " + path);
            }
            else
            {
                _globalFileSet.Add(Path.GetFullPath(path));
            }
        }

        /// <summary>
        /// Adds all the referenced files to the driver's global folder.
        /// </summary>
        /// <remarks>Empty strings are quietly ignored.</remarks>
        /// <param name="paths"></param>
        internal void AddToGlobalFiles(IEnumerable<string> paths)
        {
            foreach (var path in paths)
            {
                AddToGlobalFiles(path);
            }
        }

        /// <summary>
        /// Copies the files captured to the right places, given that driverFolderPath points to the root folder of the driver.
        /// </summary>
        /// <param name="driverFolderPath"></param>
        internal void CopyToDriverFolder(string driverFolderPath)
        {
            var localFolderPath = Path.Combine(driverFolderPath, _fileNames.GetLocalFolderPath());
            CopyAllToFolder(_localFileSet, localFolderPath);
            var globalFolderPath = Path.Combine(driverFolderPath, _fileNames.GetGlobalFolderPath());
            CopyAllToFolder(_globalFileSet, globalFolderPath);
        }

        /// <summary>
        /// Adds all the files referenced in the given JobSubmission
        /// </summary>
        /// <param name="submission"></param>
        internal void AddJobFiles(IJobSubmission submission)
        {
            AddToGlobalFiles(submission.GlobalFiles);
            AddToGlobalFiles(submission.GlobalAssemblies);
            AddToLocalFiles(submission.LocalFiles);
            AddToLocalFiles(submission.LocalAssemblies);
        }

        /// <summary>
        /// Copies all the files referenced in sourcePaths to destinationPath
        /// </summary>
        /// <param name="sourcePaths"></param>
        /// <param name="destinationPath"></param>
        private static void CopyAllToFolder(IEnumerable<string> sourcePaths, string destinationPath)
        {
            foreach (var file in sourcePaths)
            {
                var destinationFileName = Path.GetFileName(file);
                var destination = Path.Combine(destinationPath, destinationFileName);
                Logger.Log(Level.Verbose, "Copying {0} to {1}", file, destination);
                File.Copy(file, destination);
            }
        }
    }

    /// <summary>
    /// An EqualityComparer for strings that ignores the case when comparing.
    /// </summary>
    internal sealed class CaseIgnoringStringEqualityComparer : IEqualityComparer<string>
    {
        internal static CaseIgnoringStringEqualityComparer TheInstance = new CaseIgnoringStringEqualityComparer();

        public bool Equals(string stringOne, string stringTwo)
        {
            return stringOne.ToLower().Equals(stringTwo.ToLower());
        }

        public int GetHashCode(string s)
        {
            return s.ToLower().GetHashCode();
        }
    }
}