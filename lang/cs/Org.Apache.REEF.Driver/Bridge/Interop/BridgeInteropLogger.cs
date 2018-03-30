// Licensed to the Apache Software Founda;tion (ASF) under one
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
using System.Threading;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Bridge.Interop
{
    [Private]
    public static class BridgeInteropLogger
    {
        /// Local logger
        private static readonly Logger _logger = Logger.GetLogger(typeof(BridgeInteropLogger));
        /// Dictionary fo=r tracking instances.
        private static ConcurrentDictionary<Int32, BridgeLogger> _interopLoggers = new ConcurrentDictionary<Int32, BridgeLogger>();
        private static Int32 _index = 0;

        /// Pinned delegates
        private static BridgeLoggerDelegates.AllocateLogger allocateLogger;
        private static BridgeLoggerDelegates.Log log;
        private static BridgeLoggerDelegates.LogStart logStart;
        private static BridgeLoggerDelegates.LogStop logStop;
        private static BridgeLoggerDelegates.LogError logError;

        public static void Initialize()
        {
            InitializeDelegates();

            BridgeLoggerLibrary.SetAllocateBridgeLoggerDelegate(allocateLogger);
            BridgeLoggerLibrary.SetLogDelegate(log);
            BridgeLoggerLibrary.SetLogStartDelegate(logStart);
            BridgeLoggerLibrary.SetLogStopDelegate(logStop);
            BridgeLoggerLibrary.SetLogErrorDelegate(logError);
            BridgeLoggerLibrary.TestBridgeLoggers();
        }

        /// Bridge logger delegate implementations
        private static Int32 AllocateBridgeLoggerImpl([MarshalAs(UnmanagedType.LPWStr)] string classname)
        {
            BridgeLogger interopLogger = BridgeLogger.GetLogger(classname);
            Int32 index = Interlocked.Increment(ref _index);
            if (true == _interopLoggers.TryAdd(index, interopLogger))
            {
                _logger.Log(Level.Info, "Successfully added interop logger for [{0}] with index [{1}]", classname, index);
            }
            else
            {
                _logger.Log(Level.Error, "Failed to add interop logger for [{0}]", classname);
            }
            return index;
        }

        private static void LogImpl(Int32 index, [MarshalAs(UnmanagedType.LPWStr)] string message)
        {
            BridgeLogger interopLogger;
            if (_interopLoggers.TryGetValue(index, out interopLogger))
            {
                interopLogger.Log(message);
            }
            else
            {
                _logger.Log(Level.Error, "Invalid logger requested for id = [{0}]", index);
            }
        }

        private static void LogStartImpl(Int32 index, [MarshalAs(UnmanagedType.LPWStr)] string message)
        {
            BridgeLogger interopLogger;
            if (_interopLoggers.TryGetValue(index, out interopLogger))
            {
                interopLogger.LogStart(message);
            }
            else
            {
                _logger.Log(Level.Error, "Invalid logger requested for id = [{0}]", index);
            }
        }

        private static void LogStopImpl(Int32 index, [MarshalAs(UnmanagedType.LPWStr)] string message)
        {
            BridgeLogger interopLogger;
            if (_interopLoggers.TryGetValue(index, out interopLogger))
            {
                interopLogger.LogStop(message);
            }
            else
            {
                _logger.Log(Level.Error, "Invalid logger requested for id = [{0}]", index);
            }
        }

        private static void LogErrorImpl(Int32 index, [MarshalAs(UnmanagedType.LPWStr)] string message,  [MarshalAs(UnmanagedType.LPWStr)] string excep)
        {
            BridgeLogger interopLogger;
            if (_interopLoggers.TryGetValue(index, out interopLogger))
            {
                interopLogger.LogError(message, new Exception(excep));
            }
            else
            {
                _logger.Log(Level.Error, "Invalid logger requested for id = [{0}]", index);
            }
        }

        static void InitializeDelegates()
        {
            allocateLogger = new BridgeLoggerDelegates.AllocateLogger(AllocateBridgeLoggerImpl);
            log = new BridgeLoggerDelegates.Log(LogImpl);
            logStart = new BridgeLoggerDelegates.LogStart(LogStartImpl);
            logStop = new BridgeLoggerDelegates.LogStop(LogStopImpl);
            logError = new BridgeLoggerDelegates.LogError(LogErrorImpl);
        }
    }
}
