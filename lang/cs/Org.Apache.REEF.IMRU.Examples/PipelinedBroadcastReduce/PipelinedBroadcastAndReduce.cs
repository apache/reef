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

using System.Globalization;
using System.IO;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
using Org.Apache.REEF.IO.PartitionedData.Random;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs;

namespace Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce
{
    /// <summary>
    /// IMRU program that performs broadcast and reduce
    /// </summary>
    public class PipelinedBroadcastAndReduce
    {
        protected readonly IIMRUClient _imruClient;

        [Inject]
        protected PipelinedBroadcastAndReduce(IIMRUClient imruClient)
        {
            _imruClient = imruClient;
        }

        /// <summary>
        /// Runs the actual broadcast and reduce job
        /// </summary>
        public void Run(int numberofMappers, int chunkSize, int numIterations, int dim, int mapperMemory, int updateTaskMemory)
        {
            var results = _imruClient.Submit<int[], int[], int[], Stream>(
                CreateJobDefinitionBuilder(numberofMappers, chunkSize, numIterations, dim, mapperMemory, updateTaskMemory)
                    .SetMapFunctionConfiguration(BuildMapperFunctionConfig())
                    .Build());
        }

        protected virtual IMRUJobDefinitionBuilder CreateJobDefinitionBuilder(int numberofMappers, int chunkSize, int numIterations, int dim, int mapperMemory, int updateTaskMemory)
        {
            return new IMRUJobDefinitionBuilder()
                    .SetUpdateFunctionConfiguration(UpdateFunctionConfig(numberofMappers, numIterations, dim))
                    .SetMapInputCodecConfiguration(MapInputCodecConfiguration())
                    .SetUpdateFunctionCodecsConfiguration(UpdateFunctionCodecsConfiguration())
                    .SetReduceFunctionConfiguration(ReduceFunctionConfiguration())
                    .SetMapInputPipelineDataConverterConfiguration(MapInputDataConverterConfig(chunkSize))
                    .SetMapOutputPipelineDataConverterConfiguration(MapOutputDataConverterConfig(chunkSize))
                    .SetPartitionedDatasetConfiguration(PartitionedDatasetConfiguration(numberofMappers))
                    .SetJobName("BroadcastReduce")
                    .SetNumberOfMappers(numberofMappers)
                    .SetMapperMemory(mapperMemory)
                    .SetUpdateTaskMemory(updateTaskMemory);
        }

        /// <summary>
        /// Configuration for Partitioned Dataset
        /// </summary>
        protected virtual IConfiguration PartitionedDatasetConfiguration(int numberofMappers)
        {
            return RandomInputDataConfiguration.ConfigurationModule.Set(RandomInputDataConfiguration.NumberOfPartitions,
                numberofMappers.ToString()).Build();
        }

        /// <summary>
        /// Configuration for Reduce Function
        /// </summary>
        protected virtual IConfiguration ReduceFunctionConfiguration()
        {
            return IMRUReduceFunctionConfiguration<int[]>.ConfigurationModule
                .Set(IMRUReduceFunctionConfiguration<int[]>.ReduceFunction,
                    GenericType<IntArraySumReduceFunction>.Class)
                .Build();
        }

        /// <summary>
        /// Configuration for Update Function
        /// </summary>
        protected virtual IConfiguration UpdateFunctionCodecsConfiguration()
        {
            return IMRUCodecConfiguration<int[]>.ConfigurationModule
                .Set(IMRUCodecConfiguration<int[]>.Codec, GenericType<IntArrayStreamingCodec>.Class)
                .Build();
        }

        /// <summary>
        /// Configuration for Map Input Codec
        /// </summary>
        protected virtual IConfiguration MapInputCodecConfiguration()
        {
            return IMRUCodecConfiguration<int[]>.ConfigurationModule
                .Set(IMRUCodecConfiguration<int[]>.Codec, GenericType<IntArrayStreamingCodec>.Class)
                .Build();
        }

        /// <summary>
        /// Configuration for Map Output Data Converter
        /// </summary>
        protected virtual IConfiguration MapOutputDataConverterConfig(int chunkSize)
        {
            var dataConverterConfig2 =
                TangFactory.GetTang()
                    .NewConfigurationBuilder(IMRUPipelineDataConverterConfiguration<int[]>.ConfigurationModule
                        .Set(IMRUPipelineDataConverterConfiguration<int[]>.MapInputPiplelineDataConverter,
                            GenericType<PipelineIntDataConverter>.Class).Build())
                    .BindNamedParameter(typeof(BroadcastReduceConfiguration.ChunkSize),
                        chunkSize.ToString(CultureInfo.InvariantCulture))
                    .Build();
            return dataConverterConfig2;
        }

        /// <summary>
        /// Configuration for Map Input Data Converter
        /// </summary>
        protected virtual IConfiguration MapInputDataConverterConfig(int chunkSize)
        {
            var dataConverterConfig1 =
                TangFactory.GetTang()
                    .NewConfigurationBuilder(IMRUPipelineDataConverterConfiguration<int[]>.ConfigurationModule
                        .Set(IMRUPipelineDataConverterConfiguration<int[]>.MapInputPiplelineDataConverter,
                            GenericType<PipelineIntDataConverter>.Class).Build())
                    .BindNamedParameter(typeof(BroadcastReduceConfiguration.ChunkSize),
                        chunkSize.ToString(CultureInfo.InvariantCulture))
                    .Build();
            return dataConverterConfig1;
        }

        /// <summary>
        /// Configuration for Update Function
        /// </summary>
        protected IConfiguration UpdateFunctionConfig(int numberofMappers, int numIterations, int dim)
        {
            var updateFunctionConfig =
                TangFactory.GetTang().NewConfigurationBuilder(BuildUpdateFunctionConfig())
                    .BindNamedParameter(typeof(BroadcastReduceConfiguration.NumberOfIterations),
                        numIterations.ToString(CultureInfo.InvariantCulture))
                    .BindNamedParameter(typeof(BroadcastReduceConfiguration.Dimensions),
                        dim.ToString(CultureInfo.InvariantCulture))
                    .BindNamedParameter(typeof(BroadcastReduceConfiguration.NumWorkers),
                        numberofMappers.ToString(CultureInfo.InvariantCulture))
                    .Build();
            return updateFunctionConfig;
        }

        protected virtual IConfiguration BuildUpdateFunctionConfig()
        {
            return IMRUUpdateConfiguration<int[], int[], int[]>.ConfigurationModule
                .Set(IMRUUpdateConfiguration<int[], int[], int[]>.UpdateFunction,
                    GenericType<BroadcastSenderReduceReceiverUpdateFunction>.Class)
                .Set(IMRUUpdateConfiguration<int[], int[], int[]>.TaskProgressReporter,
                    GenericType<BroadcastSenderReduceReceiverUpdateFunction>.Class)
                .Build();
        }

        /// <summary>
        /// Configuration for Mapper function
        /// </summary>
        protected virtual IConfiguration BuildMapperFunctionConfig()
        {
            return IMRUMapConfiguration<int[], int[]>.ConfigurationModule
                .Set(IMRUMapConfiguration<int[], int[]>.MapFunction,
                    GenericType<BroadcastReceiverReduceSenderMapFunction>.Class)
                .Build();
        }
    }
}