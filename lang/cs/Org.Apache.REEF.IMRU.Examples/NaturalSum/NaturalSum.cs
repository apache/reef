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

using System.IO;
using System.Linq;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.ResultHandler;
using Org.Apache.REEF.IO.PartitionedData.Random;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs;

namespace Org.Apache.REEF.IMRU.Examples.NaturalSum
{
    /// <summary>
    /// A simple IMRU program that caclulates the sum of natural numbers.
    /// </summary>
    /// <remarks>
    /// This example demonstrates the use of <see cref="IPerMapperConfigGenerator"/>.
    /// N mappers are instantiated, each given an integer id (1, 2, 3, ... N) with <see cref="NaturalSumPerMapperConfigGenerator"/>.
    /// The map functions simply return the mapper's ids, and the ids get summed up via the reduce function and get passed to the updater.
    /// The job finishes after this single iteration.
    /// Call the <see cref="Run"/> method to run the example with custom parameters.
    /// </remarks>
    public sealed class NaturalSum
    {
        private readonly IIMRUClient _imruClient;

        [Inject]
        private NaturalSum(IIMRUClient imruClient)
        {
            _imruClient = imruClient;
        }

        /// <summary>
        /// Runs the actual natural sum job.
        /// </summary>
        /// <returns>The result of the natural sum IMRU job.</returns>
        public int Run(int numberofMappers, string outputFile, IConfiguration fileSystemConfig)
        {
            var results = _imruClient.Submit<int, int, int, Stream>(
                new IMRUJobDefinitionBuilder()
                    .SetMapFunctionConfiguration(IMRUMapConfiguration<int, int>.ConfigurationModule
                        .Set(IMRUMapConfiguration<int, int>.MapFunction, GenericType<NaturalSumMapFunction>.Class)
                        .Build())
                    .SetUpdateFunctionConfiguration(
                        IMRUUpdateConfiguration<int, int, int>.ConfigurationModule
                            .Set(IMRUUpdateConfiguration<int, int, int>.UpdateFunction,
                                GenericType<SingleIterUpdateFunction>.Class)
                            .Build())
                    .SetMapInputCodecConfiguration(IMRUCodecConfiguration<int>.ConfigurationModule
                        .Set(IMRUCodecConfiguration<int>.Codec, GenericType<IntStreamingCodec>.Class)
                        .Build())
                    .SetPerMapConfigurations(IMRUPerMapperConfigGeneratorConfiguration.ConfigurationModule
                        .Set(IMRUPerMapperConfigGeneratorConfiguration.PerMapperConfigGenerator,
                            GenericType<NaturalSumPerMapperConfigGenerator>.Class)
                        .Build())
                    .SetUpdateFunctionCodecsConfiguration(IMRUCodecConfiguration<int>.ConfigurationModule
                        .Set(IMRUCodecConfiguration<int>.Codec, GenericType<IntStreamingCodec>.Class)
                        .Build())
                    .SetReduceFunctionConfiguration(IMRUReduceFunctionConfiguration<int>.ConfigurationModule
                        .Set(IMRUReduceFunctionConfiguration<int>.ReduceFunction,
                            GenericType<IntSumReduceFunction>.Class)
                        .Build())
                    .SetPartitionedDatasetConfiguration(
                        RandomInputDataConfiguration.ConfigurationModule.Set(
                            RandomInputDataConfiguration.NumberOfPartitions,
                            numberofMappers.ToString()).Build())
                    .SetResultHandlerConfiguration(
                        TangFactory.GetTang()
                            .NewConfigurationBuilder(fileSystemConfig)
                            .BindImplementation(GenericType<IIMRUResultHandler<int>>.Class,
                                GenericType<WriteResultHandler<int>>.Class)
                            .BindNamedParameter(typeof(ResultOutputLocation), outputFile)
                            .Build())
                    .SetJobName("NaturalSum")
                    .SetNumberOfMappers(numberofMappers)
                    .Build());

            if (results != null)
            {
                return results.First();
            }

            return -1;
        }
    }
}
