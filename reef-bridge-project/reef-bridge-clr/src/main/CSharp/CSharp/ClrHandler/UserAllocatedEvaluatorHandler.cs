using Microsoft.Reef.Driver.Context;
using Microsoft.Reef.Interop;
using Microsoft.Reef.Tasks;
using Microsoft.Tang.Formats;
using Microsoft.Tang.Interface;
using Microsoft.Tang.Util;
using System;

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
            Console.WriteLine("UserAllocatedEvaluatorHandler OnNext 1");
            IConfiguration taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "bridgeCLRTaskId")
                .Set(TaskConfiguration.Task, GenericType<HelloTask>.Class)
                .Build();
            IConfiguration contextConfiguration = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, "bridgeCLRContextId")
                .Build();
            string contextConfigurationString = ConfigurationFile.ToConfigurationString(contextConfiguration);
            string taskConfigurationString = ConfigurationFile.ToConfigurationString(taskConfiguration);
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
