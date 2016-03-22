// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.InProcess.Parameters;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.StreamingCodec;

namespace Org.Apache.REEF.IMRU.InProcess
{
    /// <summary>
    /// Implements the IMRU client API for in-process execution
    /// </summary>
    /// <remarks>
    /// This client assumes that all given Configurations can be merged in a conflict-free way.
    /// </remarks> 
    public sealed class InProcessIMRUClient : IIMRUClient
    {
        private static readonly Logger Logger =
            Logger.GetLogger(typeof(InProcessIMRUClient));

        private readonly int _numberOfMappers;

        /// <summary>
        /// Use Tang to instantiate this.
        /// </summary>
        /// <param name="numberOfMappers">The number of mappers to instantiate</param>
        [Inject]
        private InProcessIMRUClient([Parameter(typeof(NumberOfMappers))] int numberOfMappers)
        {
            Debug.Assert(numberOfMappers > 0, "Number of mappers should be greater than zero.");
            _numberOfMappers = numberOfMappers;
        }

        /// <summary>
        /// Submits the map job
        /// </summary>
        /// <typeparam name="TMapInput">The type of the side information provided to the Map function</typeparam>
        /// <typeparam name="TMapOutput">The return type of the Map function</typeparam>
        /// <typeparam name="TResult">The return type of the computation.</typeparam>
        /// <typeparam name="TDataHandler">Data Handler type</typeparam>
        /// <param name="jobDefinition">Job definition given by the user</param>
        /// <returns>The result of the job</returns>
        public IEnumerable<TResult> Submit<TMapInput, TMapOutput, TResult, TDataHandler>(IMRUJobDefinition jobDefinition)
        {
            IConfiguration overallPerMapConfig = null;
            try
            {
                overallPerMapConfig = Configurations.Merge(jobDefinition.PerMapConfigGeneratorConfig.ToArray());
            }
            catch (Exception e)
            {
                Exceptions.Throw(e, "Issues in merging PerMapCOnfigGenerator configurations", Logger);
            }

            var mergedConfig = Configurations.Merge(
                jobDefinition.ReduceFunctionConfiguration,
                jobDefinition.UpdateFunctionConfiguration,
                jobDefinition.UpdateFunctionCodecsConfiguration,
                overallPerMapConfig);

            var injector = TangFactory.GetTang().NewInjector(mergedConfig);

            ISet<IPerMapperConfigGenerator> perMapConfigGenerators =
                (ISet<IPerMapperConfigGenerator>)injector.GetNamedInstance(typeof(PerMapConfigGeneratorSet));

            injector.BindVolatileInstance(GenericType<MapFunctions<TMapInput, TMapOutput>>.Class,
                MakeMapFunctions<TMapInput, TMapOutput>(jobDefinition.MapFunctionConfiguration, jobDefinition.PartitionedDatasetConfiguration, perMapConfigGenerators));

            var runner = injector.GetInstance<IMRURunner<TMapInput, TMapOutput, TResult>>();
            return runner.Run();
        }

        /// <summary>
        /// DriverHttpEndPoint returned by IReefClient after job submission
        /// </summary>
        public IJobSubmissionResult JobSubmissionResult
        {
            get { return null; }
        }

        /// <summary>
        /// We also need IInputPartition at each map function
        /// </summary>
        /// <param name="mapConfiguration">Map configuration given by user</param>
        /// <param name="partitionedDataSetConfig">Partitioned dataset configuration</param>
        /// <param name="perMapConfigGenerators">Per map configuration generators</param>
        /// <returns></returns>
        private MapFunctions<TMapInput, TMapOutput> MakeMapFunctions<TMapInput, TMapOutput>(IConfiguration mapConfiguration, IConfiguration partitionedDataSetConfig, ISet<IPerMapperConfigGenerator> perMapConfigGenerators)
        {
            IPartitionedInputDataSet dataset =
                TangFactory.GetTang().NewInjector(partitionedDataSetConfig).GetInstance<IPartitionedInputDataSet>();

            ISet<IMapFunction<TMapInput, TMapOutput>> mappers = new HashSet<IMapFunction<TMapInput, TMapOutput>>();

            int counter = 0;
            foreach (var descriptor in dataset)
            {
                var emptyConfig = TangFactory.GetTang().NewConfigurationBuilder().Build();
                IConfiguration perMapConfig = perMapConfigGenerators.Aggregate(emptyConfig,
                    (current, configGenerator) =>
                        Configurations.Merge(current, configGenerator.GetMapperConfiguration(counter, dataset.Count)));

                var injector = TangFactory.GetTang()
                    .NewInjector(mapConfiguration, descriptor.GetPartitionConfiguration(), perMapConfig);
                mappers.Add(injector.GetInstance<IMapFunction<TMapInput, TMapOutput>>());
                counter++;
            }
            return new MapFunctions<TMapInput, TMapOutput>(mappers);
        }
    }
}