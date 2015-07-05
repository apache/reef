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

namespace Org.Apache.REEF.IMRU.API
{
    /// <summary>
    /// Use this class to create an IMRU Job Definition.
    /// </summary>
    /// <seealso cref="IMRUJobDefinition" />
    public sealed class IMRUJobDefinitionBuilder
    {
        private IConfiguration _configuration;
        private string _jobName;

        /// <summary>
        /// Set the Configuration used to instantiate the IMapFunction, IReduceFunction, IUpdateFunction and all codec instances
        /// </summary>
        /// <param name="configuration">The Configuration used to instantiate the IMapFunction instance.</param>
        /// <seealso cref="IMRUConfiguration{TMapInput,TMapOutput,TResult}" />
        /// <returns>this</returns>
        public IMRUJobDefinitionBuilder SetConfiguration(IConfiguration configuration)
        {
            _configuration = configuration;
            return this;
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
        /// Instantiate the IMRUJobDefinition.
        /// </summary>
        /// <returns>The IMRUJobDefintion configured.</returns>
        /// <exception cref="NullReferenceException">If any of the required paremeters is not set.</exception>
        public IMRUJobDefinition Build()
        {
            if (null == _configuration)
            {
                throw new NullReferenceException("Configuration can't be null.");
            }
            if (null == _jobName)
            {
                throw new NullReferenceException("JobName can't be null.");
            }
            return new IMRUJobDefinition(_configuration, _jobName);
        }
    }
}