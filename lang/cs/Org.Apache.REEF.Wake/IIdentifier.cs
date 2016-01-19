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

namespace Org.Apache.REEF.Wake
{
    /// <summary>
    /// Identifier class for REEF.
    ///
    /// Identifiers are a generic naming primitive that carry some information about
    /// the type of object that they point to. 
    ///
    /// Examples include remote sockets or filenames.
    /// </summary>
    public abstract class IIdentifier
    {
        /// <summary>
        /// Returns a hash code for the object
        /// </summary>
        /// <returns>The hash code value for this object</returns>
        public abstract override int GetHashCode();

        /// <summary>
        /// Checks that another object is equal to this object
        /// </summary>
        /// <param name="o">The object to compare</param>
        /// <returns>True if the object is the same as the object argument; false, otherwise</returns>
        public abstract override bool Equals(object o);

        /// <summary>
        /// Returns a string representation of this object
        /// </summary>
        /// <returns>A string representation of this object</returns>
        public abstract override string ToString();
    }
}
