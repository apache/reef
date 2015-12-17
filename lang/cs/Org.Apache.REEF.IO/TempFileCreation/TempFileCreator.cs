/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Globalization;
using System.IO;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IO.TempFileCreation
{
    /// <summary>
    /// This class that wraps yemp file folder and provides an implementation of temp file folder creation
    /// </summary>
    internal sealed class TempFileCreator : ITempFileCreator
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TempFileCreator));

        private readonly string _tempFileFolder;

        /// <summary>
        /// Create a file folder for given tempFileFolder name
        /// </summary>
        /// <param name="tempFileFolder"></param>
        [Inject]
        private TempFileCreator(
            [Parameter(typeof(TempFileFolder))] string tempFileFolder)
        {
            _tempFileFolder = tempFileFolder;
            if (!Directory.Exists(_tempFileFolder))
            {
                Directory.CreateDirectory(Path.GetFullPath(tempFileFolder));
            }
            Logger.Log(Level.Verbose, string.Format(CultureInfo.CurrentCulture, "Temp directory: {0} is created", Path.GetFullPath(tempFileFolder)));
        }

        /// <summary>
        /// Return a full path of a temp file name with a radom generated file name wraped with prefix and suffix
        /// and the temp folder as the file folder
        /// </summary>
        /// <param name="prefix"></param>
        /// <param name="suffix"></param>
        /// <returns></returns>
        public string GetTempFileName(string prefix, string suffix)
        {
            return Path.GetFullPath(Path.Combine(_tempFileFolder, prefix + Guid.NewGuid().ToString("N").Substring(0, 8) + suffix));
        }

        /// <summary>
        /// Create a unique subfolder under the _tempFileFolder with given prefix and suffix
        /// </summary>
        /// <param name="prefix"></param>
        /// <param name="suffix"></param>
        /// <returns></returns>
        public string CreateTempDirectory(string prefix, string suffix)
        {
            string fullPathOfSubFolder = GetTempFileName(prefix, suffix);
            if (!Directory.Exists(fullPathOfSubFolder))
            {
                Directory.CreateDirectory(fullPathOfSubFolder);
            }

            Logger.Log(Level.Verbose, string.Format(CultureInfo.CurrentCulture, "Temp directory: {0} is created", fullPathOfSubFolder));

            return fullPathOfSubFolder;
        }
    }
}