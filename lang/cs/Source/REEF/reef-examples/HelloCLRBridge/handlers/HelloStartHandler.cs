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

using System.Collections.Generic;
using Org.Apache.Reef.Common.io;
using Org.Apache.Reef.Driver;
using Org.Apache.Reef.Driver.bridge;
using Org.Apache.Reef.Driver.Bridge;
using Org.Apache.Reef.IO.Network.Naming;
using Org.Apache.Reef.Tasks;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Annotations;

namespace Org.Apache.Reef.Examples.HelloCLRBridge
{
    public class HelloStartHandler : IStartHandler
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(HelloStartHandler));

        [Inject]
        public HelloStartHandler(HttpServerPort httpServerPort)
        {
            CreateClassHierarchy();
            Identifier = "HelloStartHandler";
            LOGGER.Log(Level.Info, "HttpPort received in HelloStartHandler: " + httpServerPort.PortNumber);
        }

        public HelloStartHandler(string id)
        {
            Identifier = id;
            CreateClassHierarchy();
        }

        public string Identifier { get; set; }

        private void CreateClassHierarchy()
        {
            HashSet<string> clrDlls = new HashSet<string>();
            clrDlls.Add(typeof(IDriver).Assembly.GetName().Name);
            clrDlls.Add(typeof(ITask).Assembly.GetName().Name);
            clrDlls.Add(typeof(HelloTask).Assembly.GetName().Name);
            clrDlls.Add(typeof(INameClient).Assembly.GetName().Name);
            clrDlls.Add(typeof(NameClient).Assembly.GetName().Name);

            ClrHandlerHelper.GenerateClassHierarchy(clrDlls);
        }
    }
}
