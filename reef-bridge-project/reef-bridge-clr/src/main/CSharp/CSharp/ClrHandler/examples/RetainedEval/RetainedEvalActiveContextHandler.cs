using Microsoft.Reef.Tasks;
using Microsoft.Tang.Formats;
using Microsoft.Tang.Implementations;
using Microsoft.Tang.Interface;
using Microsoft.Tang.Util;
using System;

namespace Microsoft.Reef.Interop.Examples.RetainedEval
{
    public class RetainedEvalActiveContextHandler : IObserver<ActiveContext>
    {
        public void OnNext(ActiveContext value)
        {
            Console.WriteLine("RetainedEvalActiveContextHandler OnNext 1");

            string taskConfigurationString = string.Empty;

            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();

            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.AddConfiguration(TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "bridgeCLRShellTask_" + DateTime.Now.Ticks)
                .Set(TaskConfiguration.Task, GenericType<ShellTask>.Class)
                .Build());
            cb.BindNamedParameter(typeof(ShellTask.Command), "echo");
            
            IConfiguration taskConfiguration = cb.Build();

            taskConfigurationString = serializer.ToString(taskConfiguration);

            Console.WriteLine("task configuration string to be submitted: " + taskConfigurationString);

            value.Clr2Java.SubmitTask(taskConfigurationString);

            Console.WriteLine("RetainedEvalActiveContextHandler OnNext 2");
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }
    }
}
