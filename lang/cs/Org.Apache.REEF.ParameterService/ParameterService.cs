using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Network.Utilities;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Parameters;
using ContextConfiguration = Org.Apache.REEF.Common.Context.ContextConfiguration;

namespace Org.Apache.REEF.ParameterService
{
    public enum ElementType
    {
        Single, Double
    }

    public sealed class ParameterServiceBuilderConfiguration : ConfigurationModuleBuilder
    {
        /// <summary>
        ///  TCP Port range start
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types",
            Justification = "not applicable")] public static readonly RequiredParameter<int> StartingPort =
                new RequiredParameter<int>();

        /// <summary>
        ///  TCP Port range count
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types",
            Justification = "not applicable")] public static readonly RequiredParameter<int> PortRange =
                new RequiredParameter<int>();

        public static ConfigurationModule ConfigurationModule
        {
            get
            {
                return new TaskConfiguration().BindNamedParameter(GenericType<TcpPortRangeStart>.Class, StartingPort)
                    .BindNamedParameter(GenericType<TcpPortRangeCount>.Class, PortRange).Build();
            }
        }
    }

    public class ParameterServiceBuilder
    {
        private const int ParameterServiceCores = 0;

        private readonly int _startingPort;
        private readonly int _portRange;
        private readonly INameServer _nameServer;
        private readonly INameClient _nameClient;
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        private CommunicationType _communication = CommunicationType.Reduce;
        private SynchronizationType _synchronization = SynchronizationType.Average;
        private ElementType _element = ElementType.Single;
        private int _taskCores = 2;

        private int _numTasks;
        private int _taskMemMB;
        private int _numTables;
        private int _numRows;
        private int _numColumns;

        [Inject]
        private ParameterServiceBuilder([Parameter(typeof(TcpPortRangeStart))] int startingPort,
            [Parameter(typeof(TcpPortRangeCount))] int portRange,
            INameServer nameServer,
            INameClient nameClient,
            IEvaluatorRequestor evaluatorRequestor)
        {
            _startingPort = startingPort;
            _portRange = portRange;
            _nameServer = nameServer;
            _nameClient = nameClient;
            _evaluatorRequestor = evaluatorRequestor;
        }

        public ParameterServiceBuilder SetCommunicationType(CommunicationType communication)
        {
            _communication = communication;
            return this;
        }

        public ParameterServiceBuilder SetSynchronizationType(SynchronizationType synchronization)
        {
            _synchronization = synchronization;
            return this;
        }

        /// <summary>
        /// Number of tasks to run, possibly equal to the number of file partitions. 
        /// Will create as many evaluators as the number of tasks specified
        /// </summary>
        /// <param name="numTasks"> Number of tasks to run </param>
        /// <returns></returns>
        public ParameterServiceBuilder SetNumberOfTasks(int numTasks)
        {
            _numTasks = numTasks;
            return this;
        }

        /// <summary>
        /// Memory requirements for loading data into memory
        /// </summary>
        /// <param name="taskMemMB">required memory in MB</param>
        /// <returns></returns>
        public ParameterServiceBuilder SetTaskMemoryMB(int taskMemMB)
        {
            _taskMemMB = taskMemMB;
            return this;
        }

        /// <summary>
        /// Core requirements for users tasks
        /// </summary>
        /// <param name="taskCores">number of cores</param>
        /// <returns></returns>
        public ParameterServiceBuilder SetTaskCores(int taskCores)
        {
            _taskCores = taskCores;
            return this;
        }

        /// <summary>
        /// Number of tables of parameters to be created on the Parameter Server
        /// </summary>
        /// <param name="numTables"></param>
        /// <returns></returns>
        public ParameterServiceBuilder SetNumTables(int numTables)
        {
            _numTables = numTables;
            return this;
        }

        /// <summary>
        /// Number of rows of parameters in each table to be created on the Parameter Server
        /// </summary>
        /// <param name="numRows"></param>
        /// <returns></returns>
        public ParameterServiceBuilder SetNumRows(int numRows)
        {
            _numRows = numRows;
            return this;
        }

        /// <summary>
        /// Number of elements in each row of parameters in each table to be created on the Parameter Server
        /// </summary>
        /// <param name="numColumns"></param>
        /// <returns></returns>
        public ParameterServiceBuilder SetNumColumns(int numColumns)
        {
            _numColumns = numColumns;
            return this;
        }

        public ParameterServiceBuilder SetElementType(ElementType element)
        {
            _element = element;
            return this;
        }

        public IParameterService Build()
        {
            if (!RequiredParametersSet())
                throw new IllegalStateException(
                    "Number of Tasks, Number of Tables, Number of Rows, Number of Columns & Task Memory MB are required parameters");
            var totalMemoryPerTask = _taskMemMB + GetMemoryInMBForParameterService();
            var totalCoresPerTask = _taskCores + GetCoresForParameterService();
            var evaluatorRequest = _evaluatorRequestor.NewBuilder()
                .SetCores(totalCoresPerTask)
                .SetMegabytes(totalMemoryPerTask)
                .SetNumber(_numTasks).Build();
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
            return new ParameterService(tcpPortProviderConfig, 
                namingConfig,
                _numTasks,
                _numTables,
                _numRows,
                _numColumns,
                _nameClient,
                _communication,
                _synchronization,
                _evaluatorRequestor,
                evaluatorRequest);
        }

        /// <summary>
        /// Another simple heuristic for computing cores requirement for parameter service
        /// Currently assuming it to be zero
        /// </summary>
        /// <returns>The number of cores required for running the Parameter Service for the given
        /// configuration of parameters</returns>
        private static int GetCoresForParameterService()
        {
            return ParameterServiceCores;
        }

        /// <summary>
        /// Currently a simple heuristic to compute the memory requirements for the parameter service
        /// Client & Server - assuming double caching - hence the multiplier 2
        /// </summary>
        /// <returns>The amount of memory in MB required for running the Parameter Service for the
        /// given configuration of parameters</returns>
        private int GetMemoryInMBForParameterService()
        {
            var perElementSize = _element == ElementType.Single ? sizeof (float) : sizeof (double);
            return (int) Math.Ceiling(_numTables*_numRows*_numColumns*perElementSize*2/(double) (_numTasks*1 << 20));
        }

        private bool RequiredParametersSet()
        {
            return _numTasks != 0 && _numTables != 0 && _numRows != 0 && _numColumns != 0 && _taskMemMB != 0;
        }
    }

    [NamedParameter("The number of tasks to be launched")]
    public class NumberOfTasks : Name<int> { }
    class ParameterService : IParameterService
    {
        private readonly int _numTasks;
        private readonly int _numTables;
        private readonly int _numRows;
        private readonly int _numColumns;
        private readonly INameClient _nameClient;
        private const string TaskContextName = "TaskContext";
        private const string ServerIdPrefix = "ParameterServer";

        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ParameterService));

        private int _contextIds;
        private int _serverIds;
        private int _tasksAdded;
        private readonly object _lock = new object();
        private readonly ConcurrentQueue<Tuple<IConfiguration, IActiveContext>> _taskTuples = new ConcurrentQueue<Tuple<IConfiguration, IActiveContext>>();
        private readonly CommunicationType _communication;
        private readonly SynchronizationType _synchronization;
        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private readonly IEvaluatorRequest _evaluatorRequest;
        private readonly IConfiguration _tcpPortProviderConfig;
        private readonly IConfiguration _namingConfig;

        internal ParameterService(IConfiguration tcpPortProviderConfig,
            IConfiguration namingConfig,
            int numTasks,
            int numTables,
            int numRows,
            int numColumns,
            INameClient nameClient,
            CommunicationType communication,
            SynchronizationType synchronization,
            IEvaluatorRequestor evaluatorRequestor,
            IEvaluatorRequest evaluatorRequest)
        {
            _tcpPortProviderConfig = tcpPortProviderConfig;
            _namingConfig = namingConfig;
            _numTasks = numTasks;
            _numTables = numTables;
            _numRows = numRows;
            _numColumns = numColumns;
            _nameClient = nameClient;
            _communication = communication;
            _synchronization = synchronization;
            _evaluatorRequestor = evaluatorRequestor;
            _evaluatorRequest = evaluatorRequest;
        }

        public void RequestEvaluators()
        {
            _evaluatorRequestor.Submit(_evaluatorRequest);
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
            _taskTuples.Enqueue(new Tuple<IConfiguration, IActiveContext>(partialTaskConfig, activeContext));
            if(_taskTuples.Count!=_numTasks)
                return;

            LOGGER.Log(Level.Verbose, "Expected number of tasks have been queued. So launching the Tasks");
            var clientConf = GetParameterClientConfiguration();
            Parallel.ForEach(_taskTuples,
                tuple =>
                {
                    var userConf = tuple.Item1;
                    var actContext = tuple.Item2;
                    actContext.SubmitTask(Configurations.Merge(userConf, clientConf));
                });
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
            var serviceConfiguration =
                ServiceConfiguration.ConfigurationModule.Set(ServiceConfiguration.Services,
                    GenericType<ParameterServer>.Class).Build();
            var serverId = Interlocked.Increment(ref _serverIds);
            var idConfig =
                TangFactory.GetTang().NewConfigurationBuilder()
                    .BindNamedParameter<ParameterServerId, string>(GenericType<ParameterServerId>.Class,
                        GetServerId(serverId)).Build();

            return Configurations.Merge(serviceConfiguration, idConfig, _tcpPortProviderConfig, _namingConfig);
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
                        _communication.ToString())
                    .BindNamedParameter<ParameterClientConfig.SynchronizationType, string>(
                        GenericType<ParameterClientConfig.SynchronizationType>.Class,
                        _synchronization.ToString())
                    .BindIntNamedParam<ParameterClientConfig.NumberOfTables>(Convert.ToString(_numTables,
                        CultureInfo.InvariantCulture))
                    .BindIntNamedParam<ParameterClientConfig.NumberOfRows>(Convert.ToString(_numRows,
                        CultureInfo.InvariantCulture))
                    .BindIntNamedParam<ParameterClientConfig.NumberOfColumns>(Convert.ToString(_numColumns,
                        CultureInfo.InvariantCulture));

            return nameAssignments.Aggregate(configurationBuilder,SerializeNameAssignment).Build();
        }

        private static ICsConfigurationBuilder SerializeNameAssignment(ICsConfigurationBuilder builder, NameAssignment assignment)
        {
            var idAddressPort = new IdAddressPort(assignment.Identifier, new AddressPort(assignment.Endpoint));
            return builder.BindSetEntry<ParameterClientConfig.ServerAddresses, string>(idAddressPort.ToString());
        }
    }
}