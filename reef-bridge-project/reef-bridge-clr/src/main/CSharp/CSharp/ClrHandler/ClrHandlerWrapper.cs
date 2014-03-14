using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace Microsoft.Reef.Interop
{
    public class ClrHandlerWrapper
    {
        public static ulong CreateFromString_ClrHandler (String str)
        {
            Console.WriteLine("+ClrHandlerFromString  " + str);
            var obj = new ClrHandler(str);

            GCHandle gc = GCHandle.Alloc(obj);
            IntPtr intPtr = GCHandle.ToIntPtr(gc);
            ulong ul = (ulong)intPtr.ToInt64();
            return ul;
        }

        public static void CallMethod_ClrHandler_OnNext(ulong handle, byte[] bytes)
        {
            Console.WriteLine("tt1");
            GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);            
            ClrHandler obj = (ClrHandler)gc.Target;

            obj.OnNext(bytes);
        }
        public static void CallMethod_ClrHandler_OnNext2(ulong handle, byte[] bytes,IInteropReturnInfo ret)
        {
            try
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                ClrHandler obj = (ClrHandler)gc.Target;
                obj.OnNext(bytes);
                throw new ApplicationException("TestException");
            }
            catch (Exception ex)
            {
                ret.SetReturnCode(11);
                ret.AddExceptionString(ex.Message + ex.StackTrace);
            }
        }

 
        public static void FreeHandle(ulong handle)
        {
            GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
            gc.Free();
        }

    }
}
