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

using Org.Apache.REEF.Common.Evaluator.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Wake.Remote.Parameters;

namespace Org.Apache.REEF.Common.Io
{
    public class TcpPortConfigurationProvider : IConfigurationProvider
    {
        private readonly int _tcpPortRangeStart;
        private readonly int _tcpPortRangeCount;
        private readonly int _tcpPortRangeTryCount;
        private readonly int _tcpPortRangeTrySeed;

        [Inject]
        private TcpPortConfigurationProvider(
            [Parameter(typeof(TcpPortRangeStart))] int tcpPortRangeStart,
            [Parameter(typeof(TcpPortRangeCount))] int tcpPortRangeCount,
            [Parameter(typeof(TcpPortRangeTryCount))] int tcpPortRangeTryCount,
            [Parameter(typeof(TcpPortRangeSeed))] int tcpPortRangeTrySeed)
        {
            _tcpPortRangeStart = tcpPortRangeStart;
            _tcpPortRangeCount = tcpPortRangeCount;
            _tcpPortRangeTryCount = tcpPortRangeTryCount;
            _tcpPortRangeTrySeed = tcpPortRangeTrySeed;
        }

        IConfiguration IConfigurationProvider.GetConfiguration()
        {
            return TangFactory.GetTang().NewConfigurationBuilder()
            .BindIntNamedParam<TcpPortRangeStart>(_tcpPortRangeStart.ToString())
            .BindIntNamedParam<TcpPortRangeCount>(_tcpPortRangeCount.ToString())
            .BindIntNamedParam<TcpPortRangeTryCount>(_tcpPortRangeTryCount.ToString())
            .BindIntNamedParam<TcpPortRangeSeed>(_tcpPortRangeTrySeed.ToString())
            .BindSetEntry<EvaluatorConfigurationProviders, TcpPortConfigurationProvider, IConfigurationProvider>()
            .Build();
        }
    }
}