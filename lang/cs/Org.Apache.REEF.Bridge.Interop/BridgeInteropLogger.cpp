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

#include "BridgeInterop.h"
#include "BridgeInteropLogger.h"
#include <string>

/// Namespace usage
using namespace std;

///
/// Anonymous namespace to hold the pointers to C# delegates needed to call into C#.
///
namespace
{
    /// Delegate function pointer type definitions.
    typedef int32_t (*AllocateLoggerPtr)(wchar_t const* classname);
    typedef void (*LogPtr)(int32_t index, wchar_t const* message);
    typedef void (*LogStartPtr)(int32_t index, wchar_t const* message);
    typedef void (*LogStopPtr)(int32_t index, wchar_t const* message);
    typedef void (*LogErrorPtr)(int32_t index, wchar_t const* message, wchar_t const* excep);

    typedef struct
    {
        AllocateLoggerPtr   allocateLogger = 0;
        LogPtr              log = 0;
        LogStartPtr         logStart = 0;
        LogStopPtr          logStop = 0;
        LogErrorPtr         logError = 0;
    } BridgeLoggerDelegates;

    BridgeLoggerDelegates delegates;
}

///
/// DLL API called from C# to set the delegates needed by bridge interop logger
/// to call the corresponding C# bridge interop logger methods.
///
extern  "C"
{
    BRIDGE_INTEROP_API void SetAllocateBridgeLoggerDelegate(AllocateLoggerPtr allocateLogger)
    {
        delegates.allocateLogger = allocateLogger;
    }

    BRIDGE_INTEROP_API void SetLogDelegate(LogPtr log)
    {
        delegates.log = log;
    }

    BRIDGE_INTEROP_API void SetLogStartDelegate(LogStartPtr logStart)
    {
        delegates.logStart = logStart;
    }

    BRIDGE_INTEROP_API void SetLogStopDelegate(LogStopPtr logStop)
    {
        delegates.logStop = logStop;
    }

    BRIDGE_INTEROP_API void SetLogErrorDelegate(LogErrorPtr logError)
    {
        delegates.logError = logError;
    }
}

///
/// Bridge interop logger method implementation.
///

Org::Apache::REEF::Driver::Bridge::BridgeInteropLogger::BridgeInteropLogger(wstring classname)
{
    _index = delegates.allocateLogger(classname.c_str());
}

Org::Apache::REEF::Driver::Bridge::BridgeInteropLogger::~BridgeInteropLogger()
{
}

/// Logging methods

void Org::Apache::REEF::Driver::Bridge::BridgeInteropLogger::Log(wstring const& message)
{
    delegates.log(_index, message.c_str());
}

void Org::Apache::REEF::Driver::Bridge::BridgeInteropLogger::LogStart(wstring const& message)
{
    delegates.logStart(_index, message.c_str());
}

void Org::Apache::REEF::Driver::Bridge::BridgeInteropLogger::LogStop(wstring const& message)
{
    delegates.logStop(_index, message.c_str());
}

void Org::Apache::REEF::Driver::Bridge::BridgeInteropLogger::LogError(wstring const& message, wstring const& excep)
{
    delegates.logError(_index, message.c_str(), excep.c_str());
}
