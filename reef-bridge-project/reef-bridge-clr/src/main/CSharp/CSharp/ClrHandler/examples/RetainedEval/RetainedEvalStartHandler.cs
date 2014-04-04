using Microsoft.Reef.Driver.Context;
using Microsoft.Reef.Tasks;
using System;
using System.Collections.Generic;

namespace Microsoft.Reef.Interop.Examples.RetainedEval
{
    public class RetainedEvalStartHandler
    {
        internal static ClrSystemHandler<AllocatedEvaluator> _allocatedEvaluatorHandler;
        internal static ClrSystemHandler<ActiveContext> _activeContextHandler;

        public static IList<ulong> GetHandlers()
        {
            List<ulong> handlers = new List<ulong>();

            CreateClassHierarchy();

            // add allocated evaluator handler
            _allocatedEvaluatorHandler = new ClrSystemHandler<AllocatedEvaluator>();
            handlers.Add(HandlerHelper.Create_Handler(_allocatedEvaluatorHandler));
            Console.WriteLine("_allocatedEvaluatorHandler added");
            _allocatedEvaluatorHandler.Subscribe(new RetainedEvalAllocatedEvaluatorHandler());

            // add active context handler
            _activeContextHandler = new ClrSystemHandler<ActiveContext>();
            handlers.Add(HandlerHelper.Create_Handler(_activeContextHandler));
            Console.WriteLine("_activeContextHandler added");
            _activeContextHandler.Subscribe(new RetainedEvalActiveContextHandler());

            return handlers;
        }

        private static void CreateClassHierarchy()
        {
            List<string> clrDlls = new List<string>();
            clrDlls.Add(typeof(ContextConfigurationOptions).Assembly.GetName().Name);
            clrDlls.Add(typeof(ITask).Assembly.GetName().Name);
            clrDlls.Add(typeof(ShellTask).Assembly.GetName().Name);

            HandlerHelper.GenerateClassHierarchy(clrDlls);
        }
    }
}
