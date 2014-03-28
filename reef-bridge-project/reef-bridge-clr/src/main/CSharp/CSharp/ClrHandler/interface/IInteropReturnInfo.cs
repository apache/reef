using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Microsoft.Reef.Interop
{
    public interface IInteropReturnInfo
    {
        void AddExceptionString(String exceptionString);       
        Boolean HasExceptions();
        void SetReturnCode(int rc);
        int GetReturnCode();
    }
}
