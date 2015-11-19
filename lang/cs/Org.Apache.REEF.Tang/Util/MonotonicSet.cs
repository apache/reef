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
using System.Collections.Generic;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Util
{
    public class MonotonicSet<T> : SortedSet<T>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(MonotonicSet<T>));

        // private static readonly long serialVersionUID = 1L;
        public MonotonicSet() : base()
        {
        }

        ////public MonotonicSet(SortedSet<T> c) : base(c.Comparer)
        ////{
        ////   AddAll(c);
        ////}

        public MonotonicSet(ICollection<T> c)
            : base(c)
        {
        }

        public MonotonicSet(IComparer<T> c)
            : base(c)
        {
        }

        public new bool Add(T e) // TODO
        {
            if (this.Contains(e))
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ArgumentException("Attempt to re-add " + e
                    + " to MonotonicSet!"), LOGGER);
            }
            return base.Add(e);
        }

        public bool AddAll(ICollection<T> c)
        {
            foreach (T t in c)
            {
                this.Add(t);
            }
            return c.Count != 0;
        }

        public bool ContainsAll(ICollection<T> c)
        {
            foreach (T t in c)
            {
                if (!this.Contains(t))
                {
                    return false;
                }
            }
            return true;
        }

        public bool AddAllIgnoreDuplicates(ICollection<T> c) 
        {
            bool ret = false;
            foreach (T t in c) 
            {
                if (!Contains(t)) 
                {
                    Add(t);
                    ret = true;
                }
            }
            return ret;
        }

        public override void Clear() 
        {
            Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new NotSupportedException("Attempt to clear MonotonicSet!"), LOGGER);
        }

        public bool Remove(Object o)
        {
            Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new NotSupportedException("Attempt to remove " + o
                + " from MonotonicSet!"), LOGGER);
            return false;
        }
    }
}
