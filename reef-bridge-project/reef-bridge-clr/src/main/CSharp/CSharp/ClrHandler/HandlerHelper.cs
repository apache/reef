using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

using Microsoft.Tang.Implementations;
using Microsoft.Tang.Interface;
using Microsoft.Tang.Protobuf;

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

        public static void GenerateClassHierarchy(List<string> clrDlls)
        {
            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(clrDlls.ToArray());
            ProtocolBufferClassHierarchy.Serialize(Constants.ClassHierarachyBin, ns);

            Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "Class hierarchy written to [{0}].", Path.Combine(Directory.GetCurrentDirectory(), Constants.ClassHierarachyBin)));
        }
    }
}
