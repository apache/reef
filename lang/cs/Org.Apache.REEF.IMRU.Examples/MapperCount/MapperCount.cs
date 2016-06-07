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
using System.IO;
using System.Linq;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.IMRU.OnREEF.ResultHandler;
using Org.Apache.REEF.IO.FileSystem.Local;
using Org.Apache.REEF.IO.PartitionedData.Random;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs;

namespace Org.Apache.REEF.IMRU.Examples.MapperCount
{
    /// <summary>
    /// A simple IMRU program that counts the number of map function instances launched.
    /// </summary>
    public sealed class MapperCount
    {
        private readonly IIMRUClient _imruClient;

        [Inject]
        private MapperCount(IIMRUClient imruClient)
        {
            _imruClient = imruClient;
        }

        /// <summary>
        /// Runs the actual mapper count job
        /// </summary>
        /// <returns>The number of MapFunction instances that are part of the job.</returns>
        public int Run(int numberofMappers, string outputFile, IConfiguration fileSystemConfig)
        {
            var results = _imruClient.Submit<int, int, int, Stream>(
                new IMRUJobDefinitionBuilder()
                    .SetMapFunctionConfiguration(IMRUMapConfiguration<int, int>.ConfigurationModule
                        .Set(IMRUMapConfiguration<int, int>.MapFunction, GenericType<IdentityMapFunction>.Class)
                        .Build())
                    .SetUpdateFunctionConfiguration(
                        IMRUUpdateConfiguration<int, int, int>.ConfigurationModule
                            .Set(IMRUUpdateConfiguration<int, int, int>.UpdateFunction,
                                GenericType<SingleIterUpdateFunction>.Class)
                            .Build())
                    .SetMapInputCodecConfiguration(IMRUCodecConfiguration<int>.ConfigurationModule
                        .Set(IMRUCodecConfiguration<int>.Codec, GenericType<IntStreamingCodec>.Class)
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
                    .SetMapTaskCores(2)
                    .SetUpdateTaskCores(3)
                    .SetJobName("MapperCount")
                    .SetNumberOfMappers(numberofMappers)
                    .SetMaxRetryNumberInRecovery(0)
                    .Build());

            if (results != null)
            {
                return results.First();
            }

            return -1;
        }
    }
}