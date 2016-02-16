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
using System.Linq;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.YARN
{
    /// <summary>
    /// For certain filesystem there might be two different URLs for the same resouce,
    /// depending on access using an external endpoint or from within the system.
    /// One such example is AzureBlockBlobStorageFileSystem
    /// https://reefhdi.blob.core.windows.net/reefdev/tmp/foo.txt is the external URL whereas
    /// when accessing this file from within HDInsight the URL is
    /// wasb://reefdev@reefhdi.blob.core.windows.net/tmp/foo.txt
    /// This creates a problem with Job resource upload where SubmitApplication request to YARN
    /// needs to use the local path whereas resource upload uses external URL.
    /// This interface allows users to specify logic for the conversion between external URL 
    /// and local URL
    /// </summary>
    [DefaultImplementation(typeof(IdentityResourceFileRemoteUrlToLocalUrlConverter))]
    internal interface IResourceFileRemoteUrlToLocalUrlConverter
    {
        string ConvertToLocalUrl(Uri url);
    }

    internal class IdentityResourceFileRemoteUrlToLocalUrlConverter : IResourceFileRemoteUrlToLocalUrlConverter
    {
        [Inject]
        private IdentityResourceFileRemoteUrlToLocalUrlConverter()
        {
        }

        public string ConvertToLocalUrl(Uri url)
        {
            return url.AbsoluteUri;
        }
    }
}