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

using System;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Implementations.Tang;

namespace Org.Apache.REEF.IMRU.API
{
    /// <summary>
    /// Use this class to create an IMRU Job Definition.
    /// </summary>
    /// <seealso cref="IMRUJobDefinition" />
    public sealed class IMRUJobDefinitionBuilder
    {
        private string _jobName;

        private static readonly IConfiguration EmptyConfiguration =
            TangFactory.GetTang().NewConfigurationBuilder().Build();

        /// <summary>
        /// Constructor
        /// </summary>
        public IMRUJobDefinitionBuilder()
        {
            MapInputPipelineDataConverterConfiguration = EmptyConfiguration;
            MapOutputPipelineDataConverterConfiguration = EmptyConfiguration;
            PartitionedDatasetConfiguration = EmptyConfiguration;
        }

        /// <summary>
        /// Set the name of the job.
        /// </summary>
        /// <param name="name">the name of the job</param>
        /// <returns>this</returns>
        public IMRUJobDefinitionBuilder SetJobName(string name)
        {
            _jobName = name;
            return this;
        }

        /// <summary>
        /// Configuration of map function
        /// </summary>
        public IConfiguration MapFunctionConfiguration { get; set; }

        /// <summary>
        /// Configuration of codec for TMapInput
        /// </summary>
        public IConfiguration MapInputCodecConfiguration { get; set; }

        /// <summary>
        /// Configuration of codecs needed by Update function
        /// </summary>
        public IConfiguration UpdateFunctionCodecsConfiguration { get; set; }

        /// <summary>
        /// Configuration of reduce function
        /// </summary>
        public IConfiguration ReduceFunctionConfiguration { get; set; }

        /// <summary>
        /// Configuration of update function
        /// </summary>
        public IConfiguration UpdateFunctionConfiguration { get; set; }

        /// <summary>
        /// Configuration of PipelineDataConverter for Map outout
        /// </summary>
        public IConfiguration MapOutputPipelineDataConverterConfiguration { get; set; }

        /// <summary>
        /// Configuration of PipelineDataConverter for Map Input
        /// </summary>
        public IConfiguration MapInputPipelineDataConverterConfiguration { get; set; }

        /// <summary>
        /// Configuration of partitioned dataset
        /// </summary>
        public IConfiguration PartitionedDatasetConfiguration { get; set; }

        /// <summary>
        /// Number of mappers
        /// </summary>
        /// TODO: This is duplicate in a sense that it can be determined 
        /// TODO: automatically from IPartitionedDataset. However, right now 
        /// TODO: GroupComm. instantiated in IMRUDriver needs this parameter 
        /// TODO: in constructor. This will be removed once we remove it from GroupComm. 
        public int NumberOfMappers { get; set; }

        /// <summary>
        /// Instantiate the IMRUJobDefinition.
        /// </summary>
        /// <returns>The IMRUJobDefintion configured.</returns>
        /// <exception cref="NullReferenceException">If any of the required paremeters is not set.</exception>
        public IMRUJobDefinition Build()
        {
            if (null == _jobName)
            {
                throw new NullReferenceException("JobName can't be null.");
            }

            return new IMRUJobDefinition(
                MapFunctionConfiguration,
                MapInputCodecConfiguration,
                UpdateFunctionCodecsConfiguration,
                ReduceFunctionConfiguration,
                UpdateFunctionConfiguration,
                MapOutputPipelineDataConverterConfiguration,
                MapInputPipelineDataConverterConfiguration,
                PartitionedDatasetConfiguration,
                NumberOfMappers,
                _jobName);
        }
    }
}