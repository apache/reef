using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using ClrHandler;

//using ClrHandler;

namespace Microsoft.Reef.Interop
{
    public class HandlerHelper
    {
        public static ulong Create_Handler(object allocatedEvaluatorHandler)
        {
            GCHandle gc = GCHandle.Alloc(allocatedEvaluatorHandler);
            IntPtr intPtr = GCHandle.ToIntPtr(gc);
            ulong ul = (ulong)intPtr.ToInt64();
            return ul;
        }

        public static void FreeHandle(ulong handle)
        {
            GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
            gc.Free();
        }
    }
}
