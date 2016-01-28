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

using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.Yarn
{
    [DefaultImplementation(typeof(LegacyJobResourceUploader))]
    public interface IJobResourceUploader
    {
        /// <summary>
        /// Upload archived local driver folder to DFS destination path.
        /// </summary>
        /// <param name="driverArchivePath">Local path where archived REEF application resources are staged</param>
        /// <param name="jobSubmissionDirectory">Remote directory path where we will upload resources</param>
        /// <returns>Path, modification time and size of uploaded file as JobResource</returns>
        JobResource UploadJobResource(string driverArchivePath, string jobSubmissionDirectory);
    }
}