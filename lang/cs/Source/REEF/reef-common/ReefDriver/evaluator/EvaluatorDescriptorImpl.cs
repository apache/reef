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

using Org.Apache.Reef.Common.Catalog;
using Org.Apache.Reef.Common.Evaluator;
using Org.Apache.Reef.Driver.Bridge;
using Org.Apache.Reef.Utilities.Diagnostics;
using Org.Apache.Reef.Utilities.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace Org.Apache.Reef.Driver.Evaluator
{
    public class EvaluatorDescriptorImpl : IEvaluatorDescriptor
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(EvaluatorDescriptorImpl));
        
        private INodeDescriptor _nodeDescriptor;

        private EvaluatorType _type;

        private int _megaBytes;

        private int _virtualCore;

        private string _rack = "default_rack";

        public EvaluatorDescriptorImpl(string serializedString)
        {
            FromString(serializedString);
        }

        public EvaluatorDescriptorImpl(INodeDescriptor nodeDescriptor, EvaluatorType type, int megaBytes, int core)
        {
            _nodeDescriptor = nodeDescriptor;
            _type = type;
            _megaBytes = megaBytes;
            _virtualCore = core;
        }

        public INodeDescriptor NodeDescriptor 
        {
            get
            {
                return _nodeDescriptor;
            }

            set
            {
            }
        }

        public EvaluatorType EvaluatorType
        {
            get
            {
                return _type;
            }

            set
            {
            }
        }

        public int Memory
        {
            get
            {
                return _megaBytes;
            }

            set
            {
            }
        }

        public int VirtualCore
        {
            get
            {
                return _virtualCore;
            }

            set
            {
            }
        }

        public string Rack
        {
            get
            {
                return _rack;
            }

            set
            {
            }
        }

        public void FromString(string str)
        {
            Dictionary<string, string> settings = new Dictionary<string, string>();
            string[] components = str.Split(',');
            foreach (string component in components)
            {
                string[] pair = component.Trim().Split('=');
                if (pair == null || pair.Length != 2)
                {
                    var e = new ArgumentException("invalid component to be used as key-value pair:", component);
                    Exceptions.Throw(e, LOGGER);
                }
                settings.Add(pair[0], pair[1]);
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
            int portNumber = 0;
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
            int memoryInMegaBytes = 0;
            int.TryParse(memory, out memoryInMegaBytes);

            string core;
            if (!settings.TryGetValue("Core", out core))
            {
                Exceptions.Throw(new ArgumentException("cannot find Core entry"), LOGGER);
            }
            int vCore = 0;
            int.TryParse(core, out vCore);

            IPEndPoint ipEndPoint = new IPEndPoint(IPAddress.Parse(ipAddress), portNumber);

            _nodeDescriptor = new NodeDescriptorImpl();
            _nodeDescriptor.InetSocketAddress = ipEndPoint;
            _nodeDescriptor.HostName = hostName;        
            _type = EvaluatorType.CLR;
            _megaBytes = memoryInMegaBytes;
            _virtualCore = vCore;
        }

        public void SetType(EvaluatorType type)
        {
            lock (this)
            {
                if (_type != EvaluatorType.UNDECIDED)
                {
                    var e = new InvalidOperationException("Cannot change a set evaluator type: " + _type);
                    Exceptions.Throw(e, LOGGER);
                }
                _type = type;
            }
        }

        public override bool Equals(object obj)
        {
            EvaluatorDescriptorImpl other = obj as EvaluatorDescriptorImpl;
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

        private bool EquivalentMemory(EvaluatorDescriptorImpl other)
        {
            int granularity = ClrHandlerHelper.MemoryGranularity == 0
                                  ? Constants.DefaultMemoryGranularity
                                  : ClrHandlerHelper.MemoryGranularity;
            int m1 = (Memory - 1) / granularity;
            int m2 = (other.Memory - 1 ) / granularity;
            return (m1 == m2);
        }
    }
}
