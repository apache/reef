using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using ClrHandler;

//using ClrHandler;

namespace Microsoft.Reef.Interop
{
    public class ClrSystemAllocatedEvaluatorHandlerWrapper
    {
        public static ulong CreateFromString_ClrSystemAllocatedEvaluatorHandler(ClrSystemAllocatedEvaluatorHandler allocatedEvaluatorHandler)
        {
            GCHandle gc = GCHandle.Alloc(allocatedEvaluatorHandler);
            IntPtr intPtr = GCHandle.ToIntPtr(gc);
            ulong ul = (ulong)intPtr.ToInt64();
            return ul;
        }

        public static void Call_ClrSystemAllocatedEvaluatorHandler_OnNext(ulong handle, IAllocatedEvaluaotrClr2Java clr2Java)
        {
            Console.WriteLine("Call_ClrSystemAllocatedEvaluatorHandler_OnNext");

            GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
            ClrSystemAllocatedEvaluatorHandler obj = (ClrSystemAllocatedEvaluatorHandler)gc.Target;
            obj.OnNext(new AllocatedEvaluator(clr2Java));
        }


        public static void FreeHandle(ulong handle)
        {
            GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
            gc.Free();
        }

    }
}
