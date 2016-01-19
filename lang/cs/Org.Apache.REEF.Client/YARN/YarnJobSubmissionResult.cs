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
using System.IO;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.YARN
{
    internal class YarnJobSubmissionResult : JobSubmissionResult
    {
        internal YarnJobSubmissionResult(IREEFClient reefClient, string filePath) 
            : base(reefClient, filePath)
        {
        }

        protected override string GetDriverUrl(string filepath)
        {
            return GetTrackingUrlAppId(filepath);
        }

        private string GetTrackingUrlAppId(string filepath)
        {
            if (!File.Exists(filepath))
            {
                throw new ApplicationException(string.Format("File {0} deosn't exist while trying to get tracking Uri", filepath));
            }

            using (var sr = new StreamReader(File.Open(filepath, FileMode.Open, FileAccess.Read, FileShare.Read)))
            {
                _appId = sr.ReadLine();
                var trackingUrl = sr.ReadLine();
                return "http://" + trackingUrl + "/";
            }
        }
    }
}
