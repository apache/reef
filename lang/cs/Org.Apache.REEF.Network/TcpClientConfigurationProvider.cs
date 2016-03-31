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

using Org.Apache.REEF.Common.Evaluator.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Wake.Remote.Parameters;

namespace Org.Apache.REEF.Network
{
    /// <summary>
    /// Configuration provider for TcpClient connection.
    /// </summary>
    public sealed class TcpClientConfigurationProvider : IConfigurationProvider
    {
        private readonly IConfiguration _configuration;
        
        [Inject]
        private TcpClientConfigurationProvider(
            [Parameter(typeof(ConnectionRetryCount))] int connectionRetryCount,
            [Parameter(typeof(SleepTimeInMs))] int sleepTimeInMs)
        {
            _configuration = TangFactory.GetTang().NewConfigurationBuilder()
                .BindIntNamedParam<ConnectionRetryCount>(connectionRetryCount.ToString())
                .BindIntNamedParam<SleepTimeInMs>(sleepTimeInMs.ToString())
                .BindSetEntry<EvaluatorConfigurationProviders, TcpClientConfigurationProvider, IConfigurationProvider>()
                .Build();
        }

        IConfiguration IConfigurationProvider.GetConfiguration()
        {
            return _configuration;
        }
    }
}