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

namespace Org.Apache.REEF.Utilities.Attributes
{
    /// <summary>
    /// Attribute target is used at the Interop layer. Should not be used
    /// by the user, and may change at any time without a deprecation phase.
    /// </summary>
    [AttributeUsage(AttributeTargets.All)]
    public sealed class InteropAttribute : Attribute
    {
        private readonly ISet<string> _cppFiles;

        public InteropAttribute(params string[] cppFiles)
        {
            _cppFiles = cppFiles == null ? new HashSet<string>() : new HashSet<string>(cppFiles);
        }

        /// <summary>
        /// The C++ files associated with the C# file.
        /// </summary>
        public ISet<string> CppFiles
        {
            get
            {
                return new HashSet<string>(_cppFiles);
            }
        }
    }
}
