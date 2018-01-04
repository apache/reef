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

namespace Org.Apache.REEF.Common.Files
{
    /// <summary>
    /// Utility class for dealing with Paths
    /// </summary>
    internal static class PathUtilities
    {
        /// <summary>
        /// Normalizes a path for easy comparison.
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public static string NormalizePath(string path)
        {
            return Path
                .GetFullPath(path) // Get the full path 
                .TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) // Remove trailing `/` and `\`
                .ToUpperInvariant(); // Windows ignores cases.
        }

        /// <summary>
        /// Compares the two paths *after* they have been normalized using NormalizePath.
        /// </summary>
        /// <param name="path1"></param>
        /// <param name="path2"></param>
        /// <returns></returns>
        public static bool AreNormalizedEquals(string path1, string path2)
        {
            if (path1 == null || path2 == null)
            {
                return false;
            }
            return NormalizePath(path1).Equals(NormalizePath(path2));
        }
    }
}