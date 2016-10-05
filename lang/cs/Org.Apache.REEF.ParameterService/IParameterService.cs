using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.ParameterService
{
    [DefaultImplementation(typeof(ParameterService))]
    public interface IParameterService
    {
        void RequestEvaluators();
        IConfiguration GetLocalContextConfiguration();
        IConfiguration GetParameterServerConfiguration();
        void QueueTask(IConfiguration partialTaskConfig, IActiveContext activeContext);
    }
}