using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.ParameterService.Examples
{
    class Program
    {
        const string Local = "local";
        const string Yarn = "yarn";
        const string DefaultRuntimeFolder = "REEF_LOCAL_RUNTIME";

        static void Main(string[] args)
        {
            Console.WriteLine("start running client: " + DateTime.Now);
            var runOnYarn = args.Length > 0 && bool.Parse(args[0].ToLower());
            var numNodes = args.Length > 1 ? int.Parse(args[1]) : 9;
            var startPort = args.Length > 2 ? int.Parse(args[2]) : 8900;
            var portRange = args.Length > 3 ? int.Parse(args[3]) : 1000;

            var parameterServiceBuilderConfig =
                ParameterServiceBuilderConfiguration.ConfigurationModule.Set(
                    ParameterServiceBuilderConfiguration.StartingPort,
                    Convert.ToString(startPort, CultureInfo.InvariantCulture))
                    .Set(ParameterServiceBuilderConfiguration.PortRange,
                        Convert.ToString(portRange, CultureInfo.InvariantCulture))
                    .Build();

            var driverConf = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<ParSerDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<ParSerDriver>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<ParSerDriver>.Class)
                .Set(DriverConfiguration.CustomTraceLevel, Level.Info.ToString())
                .Build();

            var taskConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindSetEntry<DriverBridgeConfigurationOptions.SetOfAssemblies, string>(
                    typeof(ParSerTask).Assembly.GetName().Name)
                .BindSetEntry<DriverBridgeConfigurationOptions.SetOfAssemblies, string>(
                    typeof(NameClient).Assembly.GetName().Name)
                .Build();

            var mergedDriverConfig = Configurations.Merge(driverConf, parameterServiceBuilderConfig, taskConfig);

            var runPlatform = runOnYarn ? Yarn : Local;
            TestRun(mergedDriverConfig, typeof(ParSerDriver), numNodes, "ParameterServerDriver", runPlatform);
        }

        internal static void TestRun(IConfiguration driverConfig,
            Type globalAssemblyType,
            int maxNumberOfEvaluator,
            string jobIdentifier = "myDriver",
            string runOnYarn = Local,
            string runtimeFolder = DefaultRuntimeFolder)
        {
            var injector =
                TangFactory.GetTang()
                    .NewInjector(GetRuntimeConfiguration(runOnYarn, maxNumberOfEvaluator, runtimeFolder));
            var reefClient = injector.GetInstance<IREEFClient>();
            var jobSubmissionBuilderFactory = injector.GetInstance<JobSubmissionBuilderFactory>();
            var jobSubmission = jobSubmissionBuilderFactory.GetJobSubmissionBuilder()
                .AddDriverConfiguration(driverConfig)
                .AddGlobalAssemblyForType(globalAssemblyType)
                .SetJobIdentifier(jobIdentifier)
                .Build();

            reefClient.SubmitAndGetJobStatus(jobSubmission);
        }

        internal static IConfiguration GetRuntimeConfiguration(string runOnYarn,
            int maxNumberOfEvaluator,
            string runtimeFolder)
        {
            switch (runOnYarn)
            {
                case Local:
                    var dir = Path.Combine(".", runtimeFolder);
                    return LocalRuntimeClientConfiguration.ConfigurationModule
                        .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators, maxNumberOfEvaluator.ToString())
                        .Set(LocalRuntimeClientConfiguration.RuntimeFolder, dir)
                        .Build();
                case Yarn:
                    return YARNClientConfiguration.ConfigurationModule.Build();
                default:
                    throw new Exception("Unknown runtime: " + runOnYarn);
            }
        }
    }
}
