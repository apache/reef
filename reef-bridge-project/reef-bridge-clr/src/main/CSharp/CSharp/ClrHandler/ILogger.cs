using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Microsoft.Reef.Interop
{
    public enum TraceLevel : int
    {
        Info = 0,
        Warn = 1,
        Error = 2,
    }
    public interface ILogger
    {
        void Log(TraceLevel traceLevel, String message);
    }
}