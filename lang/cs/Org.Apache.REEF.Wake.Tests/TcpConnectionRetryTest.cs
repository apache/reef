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
using System.IO;
using System.Net;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Parameters;
using Xunit;

namespace Org.Apache.REEF.Wake.Tests
{
    public class TcpConnectionRetryTest
    {
        /// <summary>
        /// Tests whether retry logic in RemoteConnectionRetryHandler is called.
        /// We run just client but not server and then check redirected output for 
        /// retry messages
        /// </summary>
        [Fact]
        public void TestConnectionRetries()
        {
            IPAddress localIpAddress = IPAddress.Parse("127.0.0.1");
            const int retryCount = 5;
            const int sleepTimeInMs = 500;
            const string message = "Retry - Count:";
            IPEndPoint remoteEndpoint = new IPEndPoint(localIpAddress, 8900);

            var memStream = new MemoryStream();
            var writer = new StreamWriter(memStream);
            Console.SetOut(writer);
            var config =
                TangFactory.GetTang()
                    .NewConfigurationBuilder()
                    .BindIntNamedParam<ConnectionRetryCount>(retryCount.ToString())
                    .BindIntNamedParam<SleepTimeInMs>(sleepTimeInMs.ToString())
                    .Build();
            var tmp = TangFactory.GetTang().NewInjector(config).GetInstance<ITcpClientConnectionFactory>();

            try
            {
                tmp.Connect(remoteEndpoint);
                Assert.False(true);
            }
            catch
            {
                memStream.Position = 0;
                using (var reader = new StreamReader(memStream))
                {
                    string line;
                    int counter = 0;
                    while ((line = reader.ReadLine()) != null)
                    {
                        if (line.Contains(message))
                        {
                            counter++;
                        }
                    }
                    Assert.Equal(counter, retryCount);
                }
            }
        }
    }
}      