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
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Utilities
{
    /// <summary>
    /// A convenience class that indicates whether a variable is set or not.
    /// The generic type T can either be of nullable or non-nullable type.
    /// </summary>
    [Serializable]
    public sealed class Optional<T>
    {
        private readonly bool _isSet = false;
        private readonly T _value;

        private Optional(T value)
        {
            _isSet = true;
            _value = value;
        }

        private Optional()
        {
            _value = default(T);
        }

        /// <summary>
        /// Gets the Value associated with the <see cref="Optional{T}"/> object.
        /// If empty and <see cref="T"/> is nullable, the Value will return null.
        /// If empty and <see cref="T"/> is non-nullable, the Value will return default(T).
        /// </summary>
        public T Value 
        {
            get { return _value; }
        }

        /// <summary>
        /// Creates an <see cref="Optional{T}"/> with <see cref="Value"/> of parameter <see cref="value"/>.
        /// If <see cref="T"/> is nullable and null is passed in, an <see cref="ArgumentNullException"/> will be thrown.
        /// If <see cref="T"/> is non-nullable and default(T) is passed in, a non-empty <see cref="Optional{T}"/> object will be returned.
        /// </summary>
        public static Optional<T> Of(T value)
        {
            if (value == null)
            {
                Diagnostics.Exceptions.Throw(new ArgumentNullException("value", "Passed a null value. Use OfNullable() instead"), Logger.GetLogger(typeof(Optional<T>))); 
            }
            return new Optional<T>(value);
        }

        /// <summary>
        /// Creates an empty <see cref="Optional{T}"/>.
        /// </summary>
        public static Optional<T> Empty()
        {
            return new Optional<T>();
        }

        /// <summary>
        /// Creates an <see cref="Optional{T}"/> with <see cref="Value"/> of parameter <see cref="value"/>.
        /// If <see cref="T"/> is nullable and null is passed in, an empty <see cref="Optional{T}"/> will be returned.
        /// If <see cref="T"/> is non-nullable and default(T) is passed in, a non-empty <see cref="Optional{T}"/> object will be returned.
        /// </summary>
        public static Optional<T> OfNullable(T value)
        {
            if (value == null)
            {
                return Empty();
            }
            else
            {
                return Of(value);
            }
        }

        /// <summary>
        /// Returns <see cref="other"/> if the current <see cref="Optional{T}"/> is empty.
        /// </summary>
        public T OrElse(T other)
        {
            if (IsPresent())
            {
                return Value;
            }
            else
            {
                return other;
            }
        }

        /// <summary>
        /// Returns true if the current <see cref="Optional{T}"/> is empty, false otherwise.
        /// For nullable <see cref="T"/>, the <see cref="Optional{T}"/> is empty if <see cref="Value"/> is null.
        /// For non-nullable <see cref="T"/>, the <see cref="Optional{T}"/> is empty if <see cref="Optional{T}"/> 
        /// is created with <see cref="Empty"/>.
        /// </summary>
        public bool IsPresent()
        {
            return _isSet && _value != null;
        }

        /// <summary>
        /// Tests the equality of the underlying <see cref="Value"/>.
        /// </summary>
        public override bool Equals(object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (obj == null || obj.GetType() != this.GetType())
            {
                return false;
            }
            Optional<T> optional = (Optional<T>)obj;
            if (_value != null ? !_value.Equals(optional.Value) : optional.Value != null)
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// Gets the hashcode of the underlying <see cref="Value"/>.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return _value != null ? _value.GetHashCode() : 0;
        }

        public override string ToString()
        {
            return "Optional{value=" + _value + "}";
        }
    }
}
