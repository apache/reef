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
using System.Linq;
using System.Net;
using Org.Apache.REEF.Common.Catalog;
using Org.Apache.REEF.Common.Evaluator;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Evaluator
{
    // This class is `public` because it is called from C++ code.
    [Private]
    public sealed class EvaluatorDescriptorImpl : IEvaluatorDescriptor
    {
        private const string DefaultRackName = "default_rack";
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(EvaluatorDescriptorImpl));
        private readonly int _core;
        private readonly EvaluatorType _evaluatorType;
        private readonly int _megaBytes;
        private readonly INodeDescriptor _nodeDescriptor;
        private readonly string _rack;
        private readonly string _runtimeName;

        // TODO[JIRA REEF-1054]: make runtimeName not optional
        internal EvaluatorDescriptorImpl(INodeDescriptor nodeDescriptor, EvaluatorType type, int megaBytes, int core, string rack = DefaultRackName, string runtimeName = "")
        {
            _nodeDescriptor = nodeDescriptor;
            _evaluatorType = type;
            _megaBytes = megaBytes;
            _core = core;
            _rack = rack;
            _runtimeName = runtimeName;
        }

        /// <summary>
        /// Constructor only to be used by the bridge.
        /// </summary>
        /// <param name="str"></param>
        public EvaluatorDescriptorImpl(string str)
        {
            var settings = new Dictionary<string, string>();
            var components = str.Split(',');
            foreach (var component in components)
            {
                var pair = component.Trim().Split('=');
                if (pair == null || pair.Length != 2)
                {
                    var e = new ArgumentException("invalid component to be used as key-value pair:", component);
                    Exceptions.Throw(e, LOGGER);
                }
                settings.Add(pair[0], pair[1]);
            }

            // TODO[JIRA REEF-1054]: make runtimeName not optional
            string runtimeName;
            if (!settings.TryGetValue("RuntimeName", out runtimeName))
            {
                Exceptions.Throw(new ArgumentException("cannot find RuntimeName entry"), LOGGER);
            }
            string ipAddress;
            if (!settings.TryGetValue("IP", out ipAddress))
            {
                Exceptions.Throw(new ArgumentException("cannot find IP entry"), LOGGER);
            }
            ipAddress = ipAddress.Split('/').Last();
            string port;
            if (!settings.TryGetValue("Port", out port))
            {
                Exceptions.Throw(new ArgumentException("cannot find Port entry"), LOGGER);
            }
            var portNumber = 0;
            int.TryParse(port, out portNumber);
            string hostName;
            if (!settings.TryGetValue("HostName", out hostName))
            {
                Exceptions.Throw(new ArgumentException("cannot find HostName entry"), LOGGER);
            }
            string memory;
            if (!settings.TryGetValue("Memory", out memory))
            {
                Exceptions.Throw(new ArgumentException("cannot find Memory entry"), LOGGER);
            }
            var memoryInMegaBytes = 0;
            int.TryParse(memory, out memoryInMegaBytes);

            string core;
            if (!settings.TryGetValue("Core", out core))
            {
                Exceptions.Throw(new ArgumentException("cannot find Core entry"), LOGGER);
            }
            var vCore = 0;
            int.TryParse(core, out vCore);

            var ipEndPoint = new IPEndPoint(IPAddress.Parse(ipAddress), portNumber);

            _nodeDescriptor = new NodeDescriptorImpl { InetSocketAddress = ipEndPoint, HostName = hostName };
            _evaluatorType = EvaluatorType.CLR;
            _megaBytes = memoryInMegaBytes;
            _core = vCore;
            _runtimeName = runtimeName;
        }

        public INodeDescriptor NodeDescriptor
        {
            get { return _nodeDescriptor; }
        }

        public EvaluatorType EvaluatorType
        {
            get { return _evaluatorType; }
        }

        public int Memory
        {
            get { return _megaBytes; }
        }

        public int VirtualCore
        {
            get { return _core; }
        }

        public string Rack
        {
            get { return _rack; }
        }

        public string RuntimeName
        {
            get { return _runtimeName; }
        }

        public override bool Equals(object obj)
        {
            var other = obj as EvaluatorDescriptorImpl;
            if (other == null)
            {
                return false;
            }

            return EquivalentMemory(other);

            // we don't care about rack now;
            // && string.Equals(_rack, other.Rack, StringComparison.OrdinalIgnoreCase);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        private bool EquivalentMemory(IEvaluatorDescriptor other)
        {
            var granularity = ClrHandlerHelper.MemoryGranularity == 0
                ? Constants.DefaultMemoryGranularity
                : ClrHandlerHelper.MemoryGranularity;
            var m1 = (Memory - 1) / granularity;
            var m2 = (other.Memory - 1) / granularity;
            return m1 == m2;
        }
    }
}