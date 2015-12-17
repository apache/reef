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

using System.Globalization;
using System.IO;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Io
{
    /// <summary>
    /// This class wraps temp file folder and temp file folder creation
    /// </summary>
    public sealed class WorkingDirectoryTempFileCreator : ITempFileCreator
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(WorkingDirectoryTempFileCreator));

        private readonly string _tempFileFolder;

        /// <summary>
        /// Create a temp file folder 
        /// </summary>
        /// <param name="tempFileFolder"></param>
        [Inject]
        private WorkingDirectoryTempFileCreator(
            [Parameter(typeof(TempFileFolder))] string tempFileFolder)
        {
            _tempFileFolder = tempFileFolder;
            if (!Directory.Exists(tempFileFolder))
            {
                Directory.CreateDirectory(tempFileFolder);
            }
            Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "Temp directory: {0} is craeted", Path.GetFullPath(tempFileFolder)));
        }

        /// <summary>
        /// returns teh temp file folder
        /// </summary>
        public string TempFileFolder
        {
            get { return _tempFileFolder; }
        }

        /// <summary>
        /// Create a temp file sub folder 
        /// </summary>
        /// <param name="subFolder"></param>
        /// <returns></returns>
        public string CreateTempDirectory(string subFolder)
        {
            string folder = _tempFileFolder + subFolder;
            if (!Directory.Exists(folder))
            {
                Directory.CreateDirectory(folder);
            }

            var path = Path.GetFullPath(folder);
            Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "Temp directory: {0} is craeted", path));

            return path;
        }
    }
}