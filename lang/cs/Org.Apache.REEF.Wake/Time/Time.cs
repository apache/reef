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

namespace Org.Apache.REEF.Wake.Time
{
    /// <summary>
    /// Time object
    /// </summary>
    public abstract class Time : IComparable<Time>
    {
        public Time(long timeStamp)
        {
            if (timeStamp < 0)
            {
                throw new ArgumentException("Time must have timeStamp of at least 0.");
            }

            TimeStamp = timeStamp;
        }

        public long TimeStamp { get; private set; }

        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}:[{1}]", GetType().Name, TimeStamp);
        }

        public override int GetHashCode()
        {
            return TimeStamp.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if (this == obj)
            {
                return true;
            }
            
            // Note: This allows for the very strange semantic where
            // two different subclasses might return true for Equals.
            var other = obj as Time;
            if (other != null)
            {
                return CompareTo(other) == 0;
            }

            return false;
        }

        public int CompareTo(Time other)
        {
            return TimeStamp.CompareTo(other.TimeStamp);
        }
    }
}
