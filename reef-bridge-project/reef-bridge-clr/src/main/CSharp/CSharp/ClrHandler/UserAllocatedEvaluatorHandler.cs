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
            //const string ClassHierarchyBinFileName = "clrClassHierarchy.bin";
            Console.WriteLine("UserAllocatedEvaluatorHandler OnNext 1");
            //List<string> taskDlls = new List<string>();
            //taskDlls.Add(typeof(ITask).Assembly.GetName().Name);
            //taskDlls.Add(typeof(HelloTask).Assembly.GetName().Name);

            //IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(taskDlls.ToArray());
            //ProtocolBufferClassHierarchy.Serialize(ClassHierarchyBinFileName, ns);

            //Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "Class hierarchy written to [{0}].", Directory.GetCurrentDirectory()));

            string contextConfigurationString = value.ContextConfigStr; 
            string taskConfigurationString = value.TaskConfigStr;

            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();

            if (contextConfigurationString.Contains("empty"))
            {         
                IConfiguration contextConfiguration = ContextConfiguration.ConfigurationModule
                    .Set(ContextConfiguration.Identifier, "bridgeHelloCLRContextId")
                    .Build();

                contextConfigurationString = serializer.ToString(contextConfiguration);
            }

            if (taskConfigurationString.Contains("empty"))
            {
                IConfiguration taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "bridgeHelloCLRTaskId")
                .Set(TaskConfiguration.Task, GenericType<HelloTask>.Class)
                .Build();

                taskConfigurationString = serializer.ToString(taskConfiguration);
            }

            Console.WriteLine("context configuration string submitted: " + contextConfigurationString);
            Console.WriteLine("task configuration string submitted: " + taskConfigurationString);

            value.Clr2Java.AllocatedEvaluatorSubmitContextAndTask(contextConfigurationString, taskConfigurationString);

            Console.WriteLine("UserAllocatedEvaluatorHandler OnNext 2");
        }
    }
}
