using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Reef.Driver.Context;
using Microsoft.Reef.Tasks;
using Microsoft.Tang.Implementations;
using Microsoft.Tang.Interface;
using Microsoft.Tang.Protobuf;

namespace Microsoft.Reef.Interop.Examples.Hello
{
    public class HelloStartHandler
    {
        internal static ClrSystemHandler<AllocatedEvaluator> _allocatedEvaluatorHandler;

        public static IList<ulong> GetHandlers()
        {
            CreateClassHierarchy();

            List<ulong> handlers = new List<ulong>();

            // add allocated evaluator handler
            _allocatedEvaluatorHandler = new ClrSystemHandler<AllocatedEvaluator>();
            handlers.Add(HandlerHelper.Create_Handler(_allocatedEvaluatorHandler));
            Console.WriteLine("_allocatedEvaluatorHandler added");
            _allocatedEvaluatorHandler.Subscribe(new HelloAllocatedEvaluatorHandler());
            return handlers;
        }

        private static void CreateClassHierarchy()
        {
            List<string> clrDlls = new List<string>();
            clrDlls.Add(typeof(ContextConfigurationOptions).Assembly.GetName().Name);
            clrDlls.Add(typeof(ITask).Assembly.GetName().Name);
            clrDlls.Add(typeof(HelloTask).Assembly.GetName().Name);

            HandlerHelper.GenerateClassHierarchy(clrDlls);
        }
    }
}
