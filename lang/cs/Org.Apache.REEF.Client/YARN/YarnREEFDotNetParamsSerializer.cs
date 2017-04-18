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

using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Client.YARN
{
    /// <summary>
    /// Job/application parameter file serializer for <see cref="YarnREEFDotNetClient"/>.
    /// </summary>
    internal sealed class YarnREEFDotNetParamsSerializer
    {
        private readonly IYarnREEFDotNetAppParamsSerializer _appParamSerializer;

        private readonly IYarnREEFDotNetJobParamsSerializer _jobParamSerializer;

        [Inject]
        private YarnREEFDotNetParamsSerializer(IYarnREEFDotNetAppParamsSerializer appParamSerializer,
                                              IYarnREEFDotNetJobParamsSerializer jobParamSerializer)
        {
            _appParamSerializer = appParamSerializer;
            _jobParamSerializer = jobParamSerializer;
        }

        /// <summary>
        /// Serializes the application parameters to reef/local/app-submission-params.json.
        /// </summary>
        internal void SerializeAppFile(AppParameters appParameters, IInjector paramInjector, string localDriverFolderPath)
        {
            _appParamSerializer.SerializeAppFile(appParameters, paramInjector, localDriverFolderPath);
        }

        /// <summary>
        /// Serializes the job parameters to job-submission-params.json.
        /// </summary>
        internal void SerializeJobFile(JobParameters jobParameters, string localDriverFolderPath, string jobSubmissionDirectory)
        {
            _jobParamSerializer.SerializeJobFile(jobParameters, localDriverFolderPath, jobSubmissionDirectory);
        }
    }
}
