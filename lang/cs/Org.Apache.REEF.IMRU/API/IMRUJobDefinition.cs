/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System.Collections.Generic;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.IMRU.API
{
    /// <summary>
    /// Describes an IMRU Job.
    /// </summary>
    /// <seealso cref="IMRUJobDefinitionBuilder" />
    public sealed class IMRUJobDefinition
    {
        private readonly string _jobName;
        private readonly IConfiguration _mapFunctionConfiguration;
        private readonly IConfiguration _mapInputCodecConfiguration;
        private readonly IConfiguration _updateFunctionCodecsConfiguration;
        private readonly IConfiguration _reduceFunctionConfiguration;
        private readonly IConfiguration _updateFunctionConfiguration;
        private readonly IConfiguration _mapOutputPipelineDataConverterConfiguration;
        private readonly IConfiguration _mapInputPipelineDataConverterConfiguration;
        private readonly IConfiguration _partitionedDatasetConfiguration;
        private readonly int _numberOfMappers;
        private readonly int _memoryPerMapper;
        private readonly int _updateTaskMemory;
        private readonly ISet<IConfiguration> _perMapConfigGeneratorConfig;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mapFunctionConfiguration">Map function configuration</param>
        /// <param name="mapInputCodecConfiguration">Map input codec configuration</param>
        /// <param name="updateFunctionCodecsConfiguration">codec configuration for update 
        /// function. It is union of TMapInput, TMapOutput and TResult configuration</param>
        /// <param name="reduceFunctionConfiguration">Reduce function configuration</param>
        /// <param name="updateFunctionConfiguration">Update function configuration</param>
        /// <param name="mapOutputPipelineDataConverterConfiguration">Configuration of 
        /// PipelineDataConverter for TMapOutput</param>
        /// <param name="mapInputPipelineDataConverterConfiguration">Configuration of 
        /// PipelineDataConverter for TMapInput</param>
        /// <param name="partitionedDatasetConfiguration">Configuration of partitioned 
        /// dataset</param>
        /// <param name="perMapConfigGeneratorConfig">Per mapper configuration</param>
        /// <param name="numberOfMappers">Number of mappers</param>
        /// <param name="memoryPerMapper">Per Mapper memory.</param>
        /// <param name="jobName">Job name</param>
        internal IMRUJobDefinition(
            IConfiguration mapFunctionConfiguration,
            IConfiguration mapInputCodecConfiguration,
            IConfiguration updateFunctionCodecsConfiguration,
            IConfiguration reduceFunctionConfiguration,
            IConfiguration updateFunctionConfiguration,
            IConfiguration mapOutputPipelineDataConverterConfiguration,
            IConfiguration mapInputPipelineDataConverterConfiguration,
            IConfiguration partitionedDatasetConfiguration,
            ISet<IConfiguration> perMapConfigGeneratorConfig,
            int numberOfMappers,
            int memoryPerMapper,
            int updateTaskMemory,
            string jobName)
        {
            _mapFunctionConfiguration = mapFunctionConfiguration;
            _mapInputCodecConfiguration = mapInputCodecConfiguration;
            _updateFunctionCodecsConfiguration = updateFunctionCodecsConfiguration;
            _reduceFunctionConfiguration = reduceFunctionConfiguration;
            _updateFunctionConfiguration = updateFunctionConfiguration;
            _mapOutputPipelineDataConverterConfiguration = mapOutputPipelineDataConverterConfiguration;
            _mapInputPipelineDataConverterConfiguration = mapInputPipelineDataConverterConfiguration;
            _partitionedDatasetConfiguration = partitionedDatasetConfiguration;
            _numberOfMappers = numberOfMappers;
            _jobName = jobName;
            _memoryPerMapper = memoryPerMapper;
            _updateTaskMemory = updateTaskMemory;
            _perMapConfigGeneratorConfig = perMapConfigGeneratorConfig;
        }

        /// <summary>
        /// Name of the job
        /// </summary>
        internal string JobName
        {
            get { return _jobName; }
        }

        /// <summary>
        /// Configuration of map function
        /// </summary>
        internal IConfiguration MapFunctionConfiguration
        {
            get { return _mapFunctionConfiguration; }
        }

        /// <summary>
        /// Configuration of codec for TMapInput
        /// </summary>
        internal IConfiguration MapInputCodecConfiguration
        {
            get { return _mapInputCodecConfiguration; }
        }

        /// <summary>
        /// Configuration of codecs needed by Update function
        /// </summary>
        internal IConfiguration UpdateFunctionCodecsConfiguration
        {
            get { return _updateFunctionCodecsConfiguration; }
        }

        /// <summary>
        /// Configuration of reduce function
        /// </summary>
        internal IConfiguration ReduceFunctionConfiguration
        {
            get { return _reduceFunctionConfiguration; }
        }

        /// <summary>
        /// Configuration of update function
        /// </summary>
        internal IConfiguration UpdateFunctionConfiguration
        {
            get { return _updateFunctionConfiguration; }
        }

        /// <summary>
        /// Configuration of PipelineDataConverter for Map outout
        /// </summary>
        internal IConfiguration MapOutputPipelineDataConverterConfiguration
        {
            get { return _mapOutputPipelineDataConverterConfiguration; }
        }

        /// <summary>
        /// Configuration of PipelineDataConverter for Map Input
        /// </summary>
        internal IConfiguration MapInputPipelineDataConverterConfiguration
        {
            get { return _mapInputPipelineDataConverterConfiguration; }
        }

        /// <summary>
        /// Configuration of partitioned dataset
        /// </summary>
        internal IConfiguration PartitionedDatasetConfiguration
        {
            get { return _partitionedDatasetConfiguration; }
        }

        /// <summary>
        /// Number of mappers
        /// </summary>
        /// TODO: This is duplicate in a sense that it can be determined 
        /// TODO: automatically from IPartitionedDataset. However, right now 
        /// TODO: GroupComm. instantiated in IMRUDriver needs this parameter 
        /// TODO: in constructor. This will be removed once we remove it from GroupComm. 
        internal int NumberOfMappers {
            get { return _numberOfMappers; }
        }

        /// <summary>
        /// Memory for each mapper in MB
        /// </summary>
        internal int MapperMemory
        {
            get { return _memoryPerMapper; }
        }

        /// <summary>
        /// Memory for update task in MB
        /// </summary>
        internal int UpdateTaskMemory
        {
            get { return _updateTaskMemory; }
        }

        /// <summary>
        /// Per mapper configuration
        /// </summary>
        internal ISet<IConfiguration> PerMapConfigGeneratorConfig
        {
            get { return _perMapConfigGeneratorConfig; }
        }
    }
}