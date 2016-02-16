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

using System.Net;
using System.Security;
using Org.Apache.REEF.Client.YARN.RestClient;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.YARN.HDI
{
    /// <summary>
    /// Provides a container around ICredential to capture HDInsight credential
    /// </summary>
    internal sealed class HDInsightCredential : IYarnRestClientCredential
    {
        private readonly ICredentials _credentials;

        [Inject]
        private HDInsightCredential(
            [Parameter(typeof(HDInsightUserName))] string userName,
            [Parameter(typeof(HDInsightPasswordSecureString))] SecureString password)
        {
            _credentials = new NetworkCredential(userName, password);
        }

        [Inject]
        private HDInsightCredential(
            [Parameter(typeof(HDInsightUserName))] string userName,
            [Parameter(typeof(HDInsightPasswordString))] string password)
        {
            _credentials = new NetworkCredential(userName, password);
        }

        public ICredentials Credentials
        {
            get { return _credentials; }
        }
    }
}