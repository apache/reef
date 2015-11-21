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
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Utilities
{
    [Serializable]
    public sealed class Optional<T>
    {
        private readonly T _value;

        private Optional(T value)
        {
            _value = value;
        }

        private Optional()
        {
            _value = default(T);
        }

        public T Value 
        {
            get { return _value; }
        }

        public static Optional<T> Of(T value)
        {
            if (value == null)
            {
                Diagnostics.Exceptions.Throw(new ArgumentNullException("value", "Passed a null value. Use OfNullable() instead"), Logger.GetLogger(typeof(Optional<T>))); 
            }
            return new Optional<T>(value);
        }

        public static Optional<T> Empty()
        {
            return new Optional<T>();
        }

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

        public bool IsPresent()
        {
            return _value != null;
        }

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
