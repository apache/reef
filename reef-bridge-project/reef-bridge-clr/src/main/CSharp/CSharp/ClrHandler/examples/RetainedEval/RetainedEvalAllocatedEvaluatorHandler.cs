using Microsoft.Reef.Driver.Context;
using Microsoft.Tang.Formats;
using Microsoft.Tang.Interface;
using System;

namespace Microsoft.Reef.Interop.Examples.RetainedEval
{
    public class RetainedEvalAllocatedEvaluatorHandler : IObserver<AllocatedEvaluator>
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
            Console.WriteLine("RetainedEvalAllocatedEvaluatorHandler OnNext 1");
            string contextConfigurationString = string.Empty;

            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();

            IConfiguration contextConfiguration = ContextConfiguration.ConfigurationModule
                    .Set(ContextConfiguration.Identifier, "RetainedEvalCLRBridgeContextId")
                    .Build();

            contextConfigurationString = serializer.ToString(contextConfiguration);

            Console.WriteLine("context configuration string to be submitted: " + contextConfigurationString);

            value.Clr2Java.SubmitContext(contextConfigurationString);

            Console.WriteLine("RetainedEvalAllocatedEvaluatorHandler OnNext 2");
        }
    }
}
