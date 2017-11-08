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

using System.IO;
using System.Threading;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.Local
{
    internal class LocalJobSubmissionResult : JobSubmissionResult
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(LocalJobSubmissionResult));

        /// <summary>
        /// Time interval between the pulling of data from the http end point file
        /// </summary>
        private const int PullInterval = 1000;

        private const string UriTemplate = @"http://{0}/";

        internal LocalJobSubmissionResult(IREEFClient reefClient, 
            string filePath, 
            int numberOfRetries, 
            int retryInterval) 
            : base(reefClient, filePath, numberOfRetries, retryInterval)
        {
        }

        protected override string GetDriverUrl(string filepath)
        {
            return GetDriverUrlForLocalRuntime(filepath);
        }

        private string GetDriverUrlForLocalRuntime(string filePath)
        {
            string fullDriverUrl = null;
            for (int i = 0; i < 10; i++)
            {
                var driverUrl = TryReadHttpServerIpAndPortFromFile(filePath);
                if (!string.IsNullOrEmpty(driverUrl))
                {
                    fullDriverUrl = string.Format(UriTemplate, driverUrl); 
                    break;
                }
                Thread.Sleep(PullInterval);
            }
            return fullDriverUrl;
        }

        private string TryReadHttpServerIpAndPortFromFile(string fileName)
        {
            string httpServerIpAndPort = null;
            try
            {
                LOGGER.Log(Level.Verbose, "try open " + fileName);
                using (var rdr = new StreamReader(File.OpenRead(fileName)))
                {
                    httpServerIpAndPort = rdr.ReadLine();
                    LOGGER.Log(Level.Verbose, "httpServerIpAndPort is " + httpServerIpAndPort);
                }
            }
            catch (FileNotFoundException)
            {
                LOGGER.Log(Level.Verbose, "File does not exist: " + fileName);
            }
            catch (IOException)
            {
                LOGGER.Log(Level.Verbose, "Encountered IOException on reading " + fileName + ".");
            }

            return httpServerIpAndPort;
        }
    }
}
