using Microsoft.Reef.Interop.Examples.Hello;
using Microsoft.Reef.Interop.Examples.RetainedEval;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

//using ClrHandler;

namespace Microsoft.Reef.Interop
{
    public class ClrSystemHandlerWrapper
    {
        public static void Call_ClrSystemAllocatedEvaluatorHandler_OnNext(ulong handle, IAllocatedEvaluaotrClr2Java clr2Java)
        {
            Console.WriteLine("Call_ClrSystemAllocatedEvaluatorHandler_OnNext");
            GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
            ClrSystemHandler<AllocatedEvaluator> obj = (ClrSystemHandler<AllocatedEvaluator>)gc.Target;
            obj.OnNext(new AllocatedEvaluator(clr2Java));
        }

        public static void Call_ClrSystemActiveContextHandler_OnNext(ulong handle, IActiveContextClr2Java clr2Java)
        {
            Console.WriteLine("Call_ClrSystemActiveContextHandler_OnNext");
            GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
            ClrSystemHandler<ActiveContext> obj = (ClrSystemHandler<ActiveContext>)gc.Target;
            obj.OnNext(new ActiveContext(clr2Java));
        }

        public static ulong[] Call_ClrSystemStartHandler_OnStart(DateTime startTime)
        {
            Console.WriteLine("*** Start time is " + startTime);
            //IList<ulong> handlers = HelloStartHandler.GetHandlers();
            IList<ulong> handlers = RetainedEvalStartHandler.GetHandlers(); 
            return handlers.ToArray();
        }
    }
}
