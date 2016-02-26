using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Network.Utilities;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Parameters;
using ContextConfiguration = Org.Apache.REEF.Common.Context.ContextConfiguration;

namespace Org.Apache.REEF.ParameterService
{
    [NamedParameter("The number of tasks to be launched")]
    public class NumberOfTasks : Name<int> { }
    class ParameterService : IParameterService
    {
        private readonly int _numTasks;
        private readonly int _startingPort;
        private readonly int _portRange;
        private readonly INameServer _nameServer;
        private readonly INameClient _nameClient;
        private const string TaskContextName = "TaskContext";
        private const string ServerIdPrefix = "ParameterServer";

        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ParameterService));

        private int _contextIds;
        private int _serverIds;
        private int _tasksAdded;
        private readonly object _lock = new object();
        private readonly List<Tuple<IConfiguration, IActiveContext>> _taskTuples = new List<Tuple<IConfiguration, IActiveContext>>();

        public CommunicationType Communication { get; set; }
        public SynchronizationType Synchronization { get; set; }

        [Inject]
        public ParameterService([Parameter(typeof(TcpPortRangeStart))] int startingPort,
            [Parameter(typeof(TcpPortRangeCount))] int portRange,
            [Parameter(typeof(NumberOfTasks))] int numTasks,
            INameServer nameServer,
            INameClient nameClient)
        {
            _startingPort = startingPort;
            _portRange = portRange;
            _numTasks = numTasks;
            _nameServer = nameServer;
            _nameClient = nameClient;
        }

        public void RequestEvaluators()
        {
            throw new System.NotImplementedException();
        }

        /// <summary>
        /// Queues the task into the TaskStarter.
        /// 
        /// Once the correct number of tasks have been queued, the final Configuration
        /// will be generated and run on the given Active Context.
        /// </summary>
        /// <param name="partialTaskConfig">The partial task configuration containing Task
        /// identifier and Task class</param>
        /// <param name="activeContext">The Active Context to run the Task on</param>
        public void QueueTask(IConfiguration partialTaskConfig, IActiveContext activeContext)
        {
            var taskId = GetTaskId(partialTaskConfig);
            LOGGER.Log(Level.Verbose, "Queuing task with identifier: " + taskId);

            lock (_lock)
            {
                _taskTuples.Add(
                    new Tuple<IConfiguration, IActiveContext>(partialTaskConfig, activeContext));

                if (Interlocked.Increment(ref _tasksAdded) != _numTasks)
                    return;
                
                LOGGER.Log(Level.Verbose, "Expected number of tasks have been queued. So launching the Tasks");
                foreach (var taskTuple in _taskTuples)
                {
                    var userConf = taskTuple.Item1;
                    var clientConf = GetParameterClientConfiguration();
                    var actContext = taskTuple.Item2;
                    actContext.SubmitTask(Configurations.Merge(userConf, clientConf));
                }
            }
        }

        public IConfiguration GetLocalContextConfiguration()
        {
            var contextNum = Interlocked.Increment(ref _contextIds);
            return
                ContextConfiguration.ConfigurationModule.Set(ContextConfiguration.Identifier,
                    string.Format(CultureInfo.InvariantCulture, "{0}-{1}", TaskContextName, contextNum)).Build();
        }

        public IConfiguration GetParameterServerConfiguration()
        {
            var serverId = Interlocked.Increment(ref _serverIds);
            var idConfig =
                TangFactory.GetTang().NewConfigurationBuilder()
                    .BindNamedParameter<ParameterServerId, string>(GenericType<ParameterServerId>.Class,
                        GetServerId(serverId)).Build();

            var tcpPortProviderConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<TcpPortRangeStart, int>(GenericType<TcpPortRangeStart>.Class,
                    _startingPort.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<TcpPortRangeCount, int>(GenericType<TcpPortRangeCount>.Class,
                    _portRange.ToString(CultureInfo.InvariantCulture))
                .Build();

            var namingConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<NamingConfigurationOptions.NameServerAddress, string>(
                    GenericType<NamingConfigurationOptions.NameServerAddress>.Class,
                    _nameServer.LocalEndpoint.Address.ToString())
                .BindNamedParameter<NamingConfigurationOptions.NameServerPort, int>(
                    GenericType<NamingConfigurationOptions.NameServerPort>.Class,
                    _nameServer.LocalEndpoint.Port.ToString(CultureInfo.InvariantCulture))
                .BindImplementation(GenericType<INameClient>.Class,
                    GenericType<NameClient>.Class).Build();

            return Configurations.Merge(idConfig, tcpPortProviderConfig, namingConfig);
        }

        /// <summary>
        /// Returns the TaskIdentifier from the Configuration.
        /// </summary>
        /// <param name="taskConfiguration">The Configuration object</param>
        /// <returns>The TaskIdentifier for the given Configuration</returns>
        public static string GetTaskId(IConfiguration taskConfiguration)
        {
            try
            {
                var injector = TangFactory.GetTang().NewInjector(taskConfiguration);
                return injector.GetNamedInstance<TaskConfigurationOptions.Identifier, string>(
                    GenericType<TaskConfigurationOptions.Identifier>.Class);
            }
            catch (InjectionException)
            {
                LOGGER.Log(Level.Error, "Unable to find task identifier");
                throw;
            }
        }

        private static string GetServerId(int serverId)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}-{1}", ServerIdPrefix, serverId);
        }


        private IConfiguration GetParameterClientConfiguration()
        {
            var serverIds = Enumerable.Range(0, _numTasks).Select(GetServerId).ToList();
            var nameAssignments = _nameClient.Lookup(serverIds);
            if (nameAssignments.Any(na => na == null))
            {
                LOGGER.Log(Level.Error,
                    "Parameter Servers {0} have not yet registered. It is an error to call this function before all servers have registered.",
                    string.Join(",",
                        Enumerable.Range(0, _numTasks)
                            .Where(i => nameAssignments[i] == null)
                            .Select(GetServerId)));
                throw new IllegalStateException("Some parameter servers have not yet registered.");
            }
            var configurationBuilder =
                TangFactory.GetTang()
                    .NewConfigurationBuilder()
                    .BindNamedParameter<ParameterClientConfig.CommunicationType, string>(
                        GenericType<ParameterClientConfig.CommunicationType>.Class,
                        Communication.ToString())
                    .BindNamedParameter<ParameterClientConfig.SynchronizationType, string>(
                        GenericType<ParameterClientConfig.SynchronizationType>.Class,
                        Synchronization.ToString());

            return nameAssignments.Aggregate(configurationBuilder,SerializeNameAssignment).Build();
        }

        private static ICsConfigurationBuilder SerializeNameAssignment(ICsConfigurationBuilder builder, NameAssignment assignment)
        {
            var idAddressPort = new IdAddressPort(assignment.Identifier, new AddressPort(assignment.Endpoint));
            return builder.BindSetEntry<ParameterClientConfig.ServerAddresses, string>(idAddressPort.ToString());
        }
    }
}