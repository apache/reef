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

using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// Helper class that deserializes the various user-provided configurations
    /// </summary>
    internal sealed class ConfigurationManager
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ConfigurationManager));

        private readonly IConfiguration _mapFunctionConfiguration;
        private readonly IConfiguration _mapInputCodecConfiguration;
        private readonly IConfiguration _updateFunctionCodecsConfiguration;
        private readonly IConfiguration _reduceFunctionConfiguration;
        private readonly IConfiguration _updateFunctionConfiguration;
        private readonly IConfiguration _mapOutputPipelineDataConverterConfiguration;
        private readonly IConfiguration _mapInputPipelineDataConverterConfiguration;

        [Inject]
        private ConfigurationManager(
            AvroConfigurationSerializer configurationSerializer,
            [Parameter(typeof(SerializedMapConfiguration))] string mapConfig,
            [Parameter(typeof(SerializedReduceConfiguration))] string reduceConfig,
            [Parameter(typeof(SerializedUpdateConfiguration))] string updateConfig,
            [Parameter(typeof(SerializedMapInputCodecConfiguration))] string mapInputCodecConfig,
            [Parameter(typeof(SerializedUpdateFunctionCodecsConfiguration))] string updateFunctionCodecsConfig,
            [Parameter(typeof(SerializedMapOutputPipelineDataConverterConfiguration))] string mapOutputPipelineDataConverterConfiguration,
            [Parameter(typeof(SerializedMapInputPipelineDataConverterConfiguration))] string mapInputPipelineDataConverterConfiguration)
        {
            _mapFunctionConfiguration = configurationSerializer.FromString(mapConfig);
            _reduceFunctionConfiguration = configurationSerializer.FromString(reduceConfig);
            _updateFunctionConfiguration = configurationSerializer.FromString(updateConfig);
            _updateFunctionCodecsConfiguration = configurationSerializer.FromString(updateFunctionCodecsConfig);
            _mapInputCodecConfiguration = configurationSerializer.FromString(mapInputCodecConfig);
            _mapOutputPipelineDataConverterConfiguration =
                configurationSerializer.FromString(mapOutputPipelineDataConverterConfiguration);
            _mapInputPipelineDataConverterConfiguration =
                configurationSerializer.FromString(mapInputPipelineDataConverterConfiguration);
        }

        /// <summary>
        /// Configuration of map function
        /// </summary>
        internal IConfiguration MapFunctionConfiguration
        {
            get { return _mapFunctionConfiguration; }
        }

        /// <summary>
        /// Configuration of Map input codec
        /// </summary>
        internal IConfiguration MapInputCodecConfiguration
        {
            get { return _mapInputCodecConfiguration; }
        }

        /// <summary>
        /// Configuration of codecs required in the Update function
        /// Union of Map input, Map output and Result codecs
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
        /// Configuration of Update function
        /// </summary>
        internal IConfiguration UpdateFunctionConfiguration
        {
            get { return _updateFunctionConfiguration; }
        }

        /// <summary>
        /// Configuration of PipelineDataConverter for chunking and dechunking Map input
        /// </summary>
        internal IConfiguration MapOutputPipelineDataConverterConfiguration
        {
            get { return _mapOutputPipelineDataConverterConfiguration; }
        }

        /// <summary>
        /// Configuration of PipelineDataConverter for chunking and dechunking Map output
        /// </summary>
        internal IConfiguration MapInputPipelineDataConverterConfiguration
        {
            get { return _mapInputPipelineDataConverterConfiguration; }
        }
    }
}