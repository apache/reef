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

using Org.Apache.REEF.Common.Evaluator.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IO.TempFileCreation
{
    /// <summary>
    /// A configuration provider for tem file
    /// </summary>
    internal sealed class TempFileConfigurationProvider : IConfigurationProvider
    {
        private readonly IConfiguration _configuration;

        /// <summary>
        /// Create configuration for for temp file
        /// </summary>
        /// <param name="tempFileFolder"></param>
        [Inject]
        private TempFileConfigurationProvider([Parameter(typeof(TempFileFolder))] string tempFileFolder)
        {
            _configuration = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<ITempFileCreator>.Class, GenericType<TempFileCreator>.Class)
                .BindStringNamedParam<TempFileFolder>(tempFileFolder)
                .BindSetEntry<EvaluatorConfigurationProviders, TempFileConfigurationProvider, IConfigurationProvider>()
                .Build();
        }

        /// <summary>
        /// Returns temp file configuration
        /// </summary>
        /// <returns></returns>
        IConfiguration IConfigurationProvider.GetConfiguration()
        {
            return _configuration;
        }
    }
}