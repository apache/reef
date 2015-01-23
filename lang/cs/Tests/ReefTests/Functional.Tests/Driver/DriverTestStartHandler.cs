﻿/**
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

using Org.Apache.Reef.Driver;
using Org.Apache.Reef.Driver.bridge;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Wake.Time;

namespace Org.Apache.Reef.Test
{
    public class DriverTestStartHandler : IStartHandler
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DriverTestStartHandler));

        private IClock _clock;
        private HttpServerPort _httpServerPort;

        [Inject]
        public DriverTestStartHandler(IClock clock, HttpServerPort httpServerPort)
        {
            _clock = clock;
            _httpServerPort = httpServerPort;
            Identifier = "DriverTestStartHandler";
            LOGGER.Log(Level.Info, "Http Server port number: " + httpServerPort.PortNumber);
        }

        public string Identifier { get; set; }
    }
}