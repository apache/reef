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
using System;
using System.Collections.Generic;

namespace Org.Apache.REEF.Client.API.Testing
{
    /// <summary>
    /// Serializable representation of a result of an assert.
    /// </summary>
    [Unstable("0.17", "Work in progress towards a new test infrastructure. See REEF-1271.")]
    internal sealed class AssertResult : IEquatable<AssertResult>
    {
        public AssertResult(string message, bool isTrue)
        {
            Message = message;
            IsTrue = isTrue;
        }

        public string Message { get; private set; }

        public bool IsTrue { get; private set; }

        public bool IsFalse
        {
            get
            {
                return !IsTrue;
            }
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as AssertResult);
        }

        public bool Equals(AssertResult other)
        {
            return other != null &&
                   Message == other.Message &&
                   IsTrue == other.IsTrue;
        }

        public override int GetHashCode()
        {
            var hashCode = -1707516999;
            hashCode = (hashCode * -1521134295) + EqualityComparer<string>.Default.GetHashCode(Message);
            hashCode = (hashCode * -1521134295) + IsTrue.GetHashCode();
            return hashCode;
        }
    }
}
