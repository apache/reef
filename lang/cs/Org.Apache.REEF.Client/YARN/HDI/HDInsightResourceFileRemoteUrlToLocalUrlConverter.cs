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

namespace Org.Apache.REEF.Client.YARN.HDI
{
    internal class HDInsightResourceFileRemoteUrlToLocalUrlConverter : IResourceFileRemoteUrlToLocalUrlConverter
    {
        [Inject]
        private HDInsightResourceFileRemoteUrlToLocalUrlConverter()
        {
        }

        public string ConvertToLocalUrl(Uri uri)
        {
            // [uri]"https://reefhdi.blob.core.windows.net/reefdev/tmp/foo.json"
            // Authority      : reefhdi.blob.core.windows.net
            // Segments       : {/, reefdev/, tmp/, foo.json}
            var container = uri.Segments[1].Trim('/');
            var path = string.Join(string.Empty, uri.Segments.Skip(2));
            return string.Format("wasb://{0}@{1}/{2}", container, uri.Authority, path);
        }
    }
}