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

namespace Org.Apache.REEF.IO.Tests
{
    /// <summary>
    /// Static methods helpful when testing IFileSystem implementations.
    /// </summary>
    internal sealed class FileSystemTestUtilities
    {
        /// <summary>
        /// Test content used when creating file system test files.
        /// </summary>
        internal const byte TestByte = 123;

        /// <summary>
        /// Compares the contents of the two files given.
        /// </summary>
        /// <param name="path1"></param>
        /// <param name="path2"></param>
        /// <returns></returns>
        internal static bool HaveSameContent(string path1, string path2)
        {
            using (var s1 = File.OpenRead(path1))
            {
                using (var s2 = File.OpenRead(path2))
                {
                    var byte1 = s1.ReadByte();
                    var byte2 = s2.ReadByte();
                    if (byte1 != byte2)
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        /// <summary>
        /// Creates a temp file and writes TestByte to it.
        /// </summary>
        /// <returns>The path to the test file.</returns>
        internal static string MakeLocalTempFile()
        {
            var result = Path.GetTempFileName();
            MakeLocalTestFile(result);
            return result;
        }

        private static void MakeLocalTestFile(string filePath)
        {
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }
            using (var s = File.Create(filePath))
            {
                s.WriteByte(TestByte);
            }
        }
    }
}