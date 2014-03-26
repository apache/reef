using Microsoft.Reef.Driver.Context;
using Microsoft.Reef.Interop;
using Microsoft.Reef.Tasks;
using Microsoft.Tang.formats;
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
            List<string> taskDlls = new List<string>();
            taskDlls.Add(typeof(ITask).Assembly.GetName().Name);
            taskDlls.Add(typeof(HelloTask).Assembly.GetName().Name);

            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(taskDlls.ToArray());
            ProtocolBufferClassHierarchy.Serialize(ClassHierarchyBinFileName, ns);

            Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "Class hierarchy written to [{0}].", Directory.GetCurrentDirectory()));

            IConfiguration taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "bridgeHelloCLRTaskId")
                .Set(TaskConfiguration.Task, GenericType<HelloTask>.Class)
                .Build();
            IConfiguration contextConfiguration = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, "bridgeHelloCLRContextId")
                .Build();
            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();

            //string contextConfigurationString = ConfigurationFile.ToConfigurationString(contextConfiguration);
            //string taskConfigurationString = ConfigurationFile.ToConfigurationString(taskConfiguration);

            string contextConfigurationString = Encoding.UTF8.GetString(serializer.ToByteArray(contextConfiguration));
            string taskConfigurationString = Encoding.UTF8.GetString(serializer.ToByteArray(taskConfiguration));

            Console.WriteLine("context configuration constructed by CLR: " + contextConfigurationString);
            Console.WriteLine("task configuration constructed by CLR: " + taskConfigurationString);

            if (value.ContextConfigStr.Contains("empty") || value.TaskConfigStr.Contains("empty"))
            {
                value.Clr2Java.AllocatedEvaluatorSubmitContextAndTask(contextConfigurationString, taskConfigurationString);
            }
            else
            {
                value.Clr2Java.AllocatedEvaluatorSubmitContextAndTask(value.ContextConfigStr, value.TaskConfigStr);
            }

            Console.WriteLine("UserAllocatedEvaluatorHandler OnNext 2");
        }
    }
}
