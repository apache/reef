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

        public static void CallMethod_ClrSystemAllocatedEvaluatorHandler_OnNext(ulong handle, IClr2Java clr2Java, byte[] bytes)
        {
            Console.WriteLine("tt2");
            GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
            ClrSystemAllocatedEvaluatorHandler obj = (ClrSystemAllocatedEvaluatorHandler)gc.Target;

            obj.OnNext(new AllocatedEvaluator(clr2Java, bytes));
        }

        public static void Call_ClrSystemAllocatedEvaluatorHandler_OnNext(ulong handle, IClr2Java clr2Java, string contextConfig, string taskConfig)
        {
            Console.WriteLine("Call_ClrSystemAllocatedEvaluatorHandler_OnNext");
            Console.WriteLine("context configuration in managed string: " + contextConfig);
            Console.WriteLine("task configuration in managed string: " + taskConfig);

            GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
            ClrSystemAllocatedEvaluatorHandler obj = (ClrSystemAllocatedEvaluatorHandler)gc.Target;
            obj.OnNext(new AllocatedEvaluator(clr2Java, contextConfig, taskConfig));
        }


        //public static void CallMethod_ClrSystemAllocatedEvaluatorHandler_OnNext2(ulong handle, byte[] bytes, IInteropReturnInfo ret)
        //{
        //    try
        //    {
        //        GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
        //        ClrSystemAllocatedEvaluatorHandler obj = (ClrSystemAllocatedEvaluatorHandler)gc.Target;
        //        obj.OnNext(new AllocatedEvaluator(bytes));
        //        throw new ApplicationException("TestException");
        //    }
        //    catch (Exception ex)
        //    {
        //        ret.SetReturnCode(11);
        //        ret.AddExceptionString(ex.Message + ex.StackTrace);
        //    }
        //}


        public static void FreeHandle(ulong handle)
        {
            GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
            gc.Free();
        }

    }
}
