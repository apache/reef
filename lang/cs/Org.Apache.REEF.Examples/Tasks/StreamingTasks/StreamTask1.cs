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

using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Examples.Tasks.StreamingTasks
{
    public class StreamTask1 : ITask
    {
        private readonly string _ipAddress;

        [Inject]
        public StreamTask1([Parameter(typeof(IpAddress))] string ipAddress)
        {
            this._ipAddress = ipAddress;
        }

        [Inject]
        public StreamTask1()
        {
        }

        public byte[] Call(byte[] memento)
        {
            System.Console.WriteLine("Hello, Streaming 1!!, Ip: " + _ipAddress);

            Thread.Sleep(10000);

            SIFirstNode();

            return null;
        }

        public void Dispose()
        {
        }

        public void SIFirstNode()
        {
            // var a = new SIFirstNodeXAM();
            // a.Process(1111, 2222, ipAddress);
        }

        [NamedParameter("Ip Address", "IP", "10.121.32.158")]
        public class IpAddress : Name<string>
        {
        }
    }
}
