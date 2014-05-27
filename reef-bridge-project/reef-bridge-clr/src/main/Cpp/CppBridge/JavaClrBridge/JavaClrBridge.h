// JavaClrBridge.h

#pragma once

using namespace System;

namespace JavaClrBridge {
    ref class LoggerWrapper
    {
    public:
        static System::Diagnostics::TraceSource^ logger = 
            gcnew System::Diagnostics::TraceSource("JavaCLRBridgeLogger", System::Diagnostics::SourceLevels::All);
        static bool initialized = false;
    };
}
