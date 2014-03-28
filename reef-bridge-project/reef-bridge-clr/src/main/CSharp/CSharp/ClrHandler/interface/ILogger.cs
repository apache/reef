using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Microsoft.Reef.Interop
{
    public enum TraceLevel : int
    {
        NoTrace = Int32.MaxValue,

        Error = 1000,
        Warning = 900,
        Info = 800,
        Verbose = 300, 
    }

    public interface ILogger
    {
        void Log(TraceLevel traceLevel, String message);
    }
}