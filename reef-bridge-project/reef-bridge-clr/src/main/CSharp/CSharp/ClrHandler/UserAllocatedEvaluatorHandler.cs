using Microsoft.Reef.Driver.Context;
using Microsoft.Reef.Interop;
using Microsoft.Reef.Tasks;
using Microsoft.Tang.Formats;
using Microsoft.Tang.Implementations;
using Microsoft.Tang.Interface;
using Microsoft.Tang.Protobuf;
using Microsoft.Tang.Util;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;

namespace ClrHandler
{
    public class UserAllocatedEvaluatorHandler : IObserver<AllocatedEvaluator>
    {

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnNext(AllocatedEvaluator value)
        {
            const string ClassHierarchyBinFileName = "clrClassHierarchy.bin";
            Console.WriteLine("UserAllocatedEvaluatorHandler OnNext 1");
            List<string> clrDlls = new List<string>();
            clrDlls.Add(typeof(ITask).Assembly.GetName().Name);
            clrDlls.Add(typeof(HelloTask).Assembly.GetName().Name);
            clrDlls.Add(typeof(ContextConfigurationOptions).Assembly.GetName().Name);

            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(clrDlls.ToArray());
            ProtocolBufferClassHierarchy.Serialize(ClassHierarchyBinFileName, ns);

            Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "Class hierarchy written to [{0}].", Path.Combine(Directory.GetCurrentDirectory(), ClassHierarchyBinFileName)));

            string contextConfigurationString = string.Empty; 
            string taskConfigurationString = string.Empty;

            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();

            IConfiguration contextConfiguration = ContextConfiguration.ConfigurationModule
                    .Set(ContextConfiguration.Identifier, "bridgeHelloCLRContextId")
                    .Build();

            IConfiguration taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "bridgeHelloCLRTaskId")
                .Set(TaskConfiguration.Task, GenericType<HelloTask>.Class)
                .Build();

            taskConfigurationString = serializer.ToString(taskConfiguration);
            contextConfigurationString = serializer.ToString(contextConfiguration);
            
            Console.WriteLine("context configuration string to be submitted: " + contextConfigurationString);
            Console.WriteLine("task configuration string to be submitted: " + taskConfigurationString);

            value.Clr2Java.SubmitContextAndTask(contextConfigurationString, taskConfigurationString);

            Console.WriteLine("UserAllocatedEvaluatorHandler OnNext 2");
        }
    }
}
