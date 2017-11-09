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

using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Client.API.Testing
{
    /// <summary>
    /// Assert methods to be used in tests of REEF and REEF applications.
    /// </summary>
    [Unstable("0.17", "Work in progress towards a new test infrastructure. See REEF-1271.")]
    public interface IAssert
    {
        /// <summary>
        /// Assert that a boolean condition is true.
        /// </summary>
        /// <param name="message">The error message for the test if condition is false.</param>
        /// <param name="condition">The condition. True indicates a passed test, false otherwise.</param>
        void True(string message, bool condition);

        /// <summary>
        /// Assert that a boolean condition is false.
        /// </summary>
        /// <param name="message">The error message for the test if condition is true.</param>
        /// <param name="condition">The condition. False indicates a passed test, true otherwise.</param>
        void False(string message, bool condition);
    }
}
