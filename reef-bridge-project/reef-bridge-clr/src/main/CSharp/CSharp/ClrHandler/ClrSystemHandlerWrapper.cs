using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using ClrHandler;

//using ClrHandler;

namespace Microsoft.Reef.Interop
{
    public class ClrSystemHandlerWrapper
    {
        public static void Call_ClrSystemAllocatedEvaluatorHandler_OnNext(ulong handle, IAllocatedEvaluaotrClr2Java clr2Java)
        {
            Console.WriteLine("Call_ClrSystemAllocatedEvaluatorHandler_OnNext");
            GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
            ClrSystemAllocatedEvaluatorHandler obj = (ClrSystemAllocatedEvaluatorHandler)gc.Target;
            obj.OnNext(new AllocatedEvaluator(clr2Java));
        }
    }
}
