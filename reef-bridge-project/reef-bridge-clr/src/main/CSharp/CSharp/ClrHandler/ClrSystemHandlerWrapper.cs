using Microsoft.Reef.Driver;
using Microsoft.Reef.Driver.Bridge;
using Microsoft.Tang.Formats;
using Microsoft.Tang.Implementations;
using Microsoft.Tang.Interface;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

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

        public static void Call_ClrSystemEvaluatorRequestor_OnNext(ulong handle, IEvaluatorRequestorClr2Java clr2Java)
        {
            Console.WriteLine("Call_ClrSystemEvaluatorRequestor_OnNext");
            GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
            ClrSystemHandler<EvaluatorRequestor> obj = (ClrSystemHandler<EvaluatorRequestor>)gc.Target;
            obj.OnNext(new EvaluatorRequestor(clr2Java));
        }

        public static void Call_ClrSystemTaskMessage_OnNext(ulong handle, ITaskMessageClr2Java clr2Java, byte[] message)
        {
            Console.WriteLine("Call_ClrSystemTaskMessage_OnNext");
            GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
            ClrSystemHandler<TaskMessage> obj = (ClrSystemHandler<TaskMessage>)gc.Target;
            obj.OnNext(new TaskMessage(clr2Java, message));
        }

        public static void Call_ClrSystemFailedTask_OnNext(ulong handle, IFailedTaskClr2Java clr2Java)
        {
            Console.WriteLine("Call_ClrSystemFailedTask_OnNext");
            GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
            ClrSystemHandler<FailedTask> obj = (ClrSystemHandler<FailedTask>)gc.Target;
            obj.OnNext(new FailedTask(clr2Java));
        }

        public static void Call_ClrSystemFailedEvaluator_OnNext(ulong handle, IFailedEvaluatorClr2Java clr2Java)
        {
            Console.WriteLine("Call_ClrSystemFailedEvaluator_OnNext");
            GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
            ClrSystemHandler<FailedEvaluator> obj = (ClrSystemHandler<FailedEvaluator>)gc.Target;
            obj.OnNext(new FailedEvaluator(clr2Java));
        }

        public static ulong[] Call_ClrSystemStartHandler_OnStart(DateTime startTime)
        {
            Console.WriteLine("*** Start time is " + startTime);

            IStartHandler startHandler;
            if (!File.Exists(Constants.ClrRuntimeConfiguration))
            {
                throw new InvalidOperationException("Cannot find CLR runtime configuration file " + Constants.ClrRuntimeConfiguration);
            }         
            try
            {
                IConfiguration clrRuntimeConfiguration = new AvroConfigurationSerializer().FromFile(Constants.ClrRuntimeConfiguration);
                IInjector injector = TangFactory.GetTang().NewInjector(clrRuntimeConfiguration);
                startHandler = injector.GetInstance<IStartHandler>();
            }
            catch (Exception e)
            {
                throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "attemp to inject starthandler encountered error {0} with message {1} and stack trace {2}", e, e.Message, e.StackTrace));
            }

            Console.WriteLine("Start handler set to be " + startHandler.Identifier);

            IList<ulong> handlers = startHandler.GetHandlers();
            return handlers.ToArray();
        }
    }
}
