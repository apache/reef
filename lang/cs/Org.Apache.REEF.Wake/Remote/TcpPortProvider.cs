/*
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
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Parameters;

namespace Org.Apache.REEF.Wake.Remote
{
    public class TcpPortProvider : ITcpPortProvider
     {
        private readonly int _tcpPortRangeStart;
        private readonly int _tcpPortRangeCount;
        private readonly int _tcpPortRangeTryCount;
        private readonly int _tcpPortRangeSeed;

        [Inject]
        internal TcpPortProvider(
            [Parameter(typeof(TcpPortRangeStart))] int tcpPortRangeStart,
            [Parameter(typeof(TcpPortRangeCount))] int tcpPortRangeCount,
            [Parameter(typeof(TcpPortRangeTryCount))] int tcpPortRangeTryCount,
            [Parameter(typeof(TcpPortRangeSeed))] int tcpPortRangeSeed)
        {
            _tcpPortRangeStart = tcpPortRangeStart;
            _tcpPortRangeCount = tcpPortRangeCount;
            _tcpPortRangeTryCount = tcpPortRangeTryCount;
            _tcpPortRangeSeed = tcpPortRangeSeed;
        }

        IEnumerator<int> IEnumerable<int>.GetEnumerator()
        {
            return new TcpPortEnumerator(_tcpPortRangeStart, _tcpPortRangeCount, _tcpPortRangeTryCount,
                _tcpPortRangeSeed);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new TcpPortEnumerator(_tcpPortRangeStart, _tcpPortRangeCount, _tcpPortRangeTryCount,
                _tcpPortRangeSeed);
        }

        private class TcpPortEnumerator : IEnumerator<int>
        {
            private readonly int _seed;
            private Random _random;
            private readonly int _tcpPortRangeStart;
            private readonly int _tcpPortRangeCount;
            private readonly int _tcpPortRangeTryCount;
            private int _tcpPortRangeAttempt;

            internal TcpPortEnumerator(
                int tcpPortRangeStart,
                int tcpPortRangeCount,
                int tcpPortRangeTryCount,
                int tcpPortRangeSeed)
            {
                _tcpPortRangeStart = tcpPortRangeStart;
                _tcpPortRangeCount = tcpPortRangeCount;
                _tcpPortRangeTryCount = tcpPortRangeTryCount;
                _tcpPortRangeAttempt = 0;
                _seed = tcpPortRangeSeed;
                _random = new Random(_seed);
            }

            int IEnumerator<int>.Current
            {
                get { return _random.Next(_tcpPortRangeCount - 1) + 1 + _tcpPortRangeStart; }
            }

            void IDisposable.Dispose()
            {
            }

            object IEnumerator.Current
            {
                get { return _random.Next(_tcpPortRangeCount - 1) + 1 + _tcpPortRangeStart; }
            }

            bool IEnumerator.MoveNext()
            {
                return _tcpPortRangeAttempt++ < _tcpPortRangeTryCount;
            }

            void IEnumerator.Reset()
            {
                _random = new Random(_seed);
            }
        }
    }
    
}