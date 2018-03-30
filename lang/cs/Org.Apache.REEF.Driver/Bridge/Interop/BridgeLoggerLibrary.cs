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

using System.Runtime.InteropServices;

namespace Org.Apache.REEF.Driver.Bridge.Interop
{
    internal static class BridgeLoggerLibrary
    {
        /// Interop library filename
        private const string INTEROP_LIBRARY = "Org.Apache.REEF.Bridge.Interop.dll";

        /// Interop library immports to set the bridge interop logger delegates in the C++ library.
        [DllImport(INTEROP_LIBRARY)]
        public static extern void SetAllocateBridgeLoggerDelegate(BridgeLoggerDelegates.AllocateLogger allocateLogger);

        [DllImport(INTEROP_LIBRARY)]
        public static extern void SetLogDelegate(BridgeLoggerDelegates.Log log);

        [DllImport(INTEROP_LIBRARY)]
        public static extern void SetLogStartDelegate(BridgeLoggerDelegates.LogStart logStart);

        [DllImport(INTEROP_LIBRARY)]
        public static extern void SetLogStopDelegate(BridgeLoggerDelegates.LogStop logStop);

        [DllImport(INTEROP_LIBRARY)]
        public static extern void SetLogErrorDelegate(BridgeLoggerDelegates.LogError logError);

        [DllImport(INTEROP_LIBRARY)]
        public static extern void TestBridgeLoggers();
    }
}
