using System;
using System.Globalization;
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.ParameterService.Examples
{
    class ParSerDriver : IObserver<IAllocatedEvaluator>,
        IObserver<IActiveContext>,
        IObserver<IDriverStarted>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ParSerDriver));

        private readonly IParameterService _parService;

        private int _taskCounter;

        [Inject]
        public ParSerDriver(IParameterService parService)
        {
            _parService = parService;
        }


        public void OnNext(IDriverStarted value)
        {
            _parService.RequestEvaluators();
        }

        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            var localContextConf = _parService.GetLocalContextConfiguration();
            var sharedContextConf = _parService.GetParameterServerConfiguration();
            allocatedEvaluator.SubmitContextAndService(localContextConf, sharedContextConf);
        }

        public void OnNext(IActiveContext activeContext)
        {
            var taskId = string.Format(CultureInfo.InvariantCulture, "Task-{0}", Interlocked.Increment(ref _taskCounter));
            var partialTaskConfig = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, taskId)
                .Set(TaskConfiguration.Task, GenericType<ParSerTask>.Class)
                .Build();
            _parService.QueueTask(partialTaskConfig, activeContext);
        }

        public void OnError(Exception error)
        {
            throw error;
        }

        public void OnCompleted()
        {
        }
    }
}
