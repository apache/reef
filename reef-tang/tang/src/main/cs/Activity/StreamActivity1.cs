/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
﻿/**
 * Copyright (C) 2012 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;
//using Microsoft.DataPipeline.ComputeService.StreamInsightTask;

namespace com.microsoft.reef.activity
{
    public class StreamActivity1 : IActivity
    {
        public string ipAddress;

        [NamedParameter("Ip Address", "IP", "10.121.32.158")]
        class IpAddress : Name<string> { }

        [Inject]
        public StreamActivity1([Parameter(Value = typeof(IpAddress))] string ipAddress)
        {
            this.ipAddress = ipAddress;
        }

        [Inject]
        public StreamActivity1()
        {
        }

        public byte[] Call(byte[] memento)
        {
            System.Console.WriteLine("Hello, Streaming 1!!, Ip: " + ipAddress);

            Thread.Sleep(10000);

            SIFirstNode();

            return null;
        }

        public static void SIFirstNode()
        {
            //var a = new SIFirstNodeXAM();
            //a.Process(1111, 2222);
        }
    }
}
