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

namespace Microsoft.Reef.Interop.Examples.Hello
{
    public class HelloAllocatedEvaluatorHandler : IObserver<AllocatedEvaluator>
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
            Console.WriteLine("HelloAllocatedEvaluatorHandler OnNext 1");
            
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

            Console.WriteLine("HelloAllocatedEvaluatorHandler OnNext 2");
        }
    }
}
