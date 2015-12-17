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

using Org.Apache.REEF.Common.Client.Parameters;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IO.TempFileCreation
{
    /// <summary>
    /// Configuration Module for TempFileCreator.
    /// </summary>
    public sealed class TempFileConfigurationModule : ConfigurationModuleBuilder
    {
        /// <summary>
        /// Temp file folder optional parameter. The default is @"./reef/tmp/" set in TempFileFolder class.
        /// </summary>
        public static readonly OptionalParameter<string> TempFileFolerParameter = new OptionalParameter<string>();

        /// <summary>
        /// It binds TempFileConfigurationProvider to DriverConfigurationProviders so that the configuration is set for 
        /// Driver configuration automatically. TempFileFolerParameter is set as an optional parameter for TempFileFolder.
        /// </summary>
        public static readonly ConfigurationModule ConfigurationModule = new TempFileConfigurationModule()
            .BindSetEntry<DriverConfigurationProviders, TempFileConfigurationProvider, IConfigurationProvider>(
                GenericType<DriverConfigurationProviders>.Class, GenericType<TempFileConfigurationProvider>.Class)
            .BindNamedParameter(GenericType<TempFileFolder>.Class, TempFileFolerParameter)
            .Build();
    }
}