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
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Bridge
{
    /// <summary>
    /// A wrapper around the general Logger class used specifically for 
    /// logging in CPP bridge code. 
    /// This is enabled when trace level is above Level.Info (included)
    /// </summary>
    [Private]
    public sealed class BridgeLogger
    {
        private readonly Logger _logger;

        public BridgeLogger(string name)
        {
            _logger = Logger.GetLogger(name);
        }

        public static BridgeLogger GetLogger(string className)
        {
            return new BridgeLogger(className);
        }

        public void Log(string message)
        {
            _logger.Log(Level.Info, message);
        }

        public void LogStart(string message)
        {
            _logger.Log(Level.Start, message);
        }

        public void LogStop(string message)
        {
            _logger.Log(Level.Stop, message);
        }

        public void LogError(string message, Exception e)
        {
            _logger.Log(Level.Error, message, e);
        }
    }
}
