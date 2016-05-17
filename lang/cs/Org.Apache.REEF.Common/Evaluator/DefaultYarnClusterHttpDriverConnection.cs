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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Utilities.Runtime.Yarn;

namespace Org.Apache.REEF.Common.Evaluator
{
    public sealed class DefaultYarnClusterHttpDriverConnection : IDriverConnection
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(DefaultYarnClusterHttpDriverConnection));

        private readonly string _applicationId;
        private readonly YarnConfiguration _yarnConfiguration;

        [Inject]
        private DefaultYarnClusterHttpDriverConnection()
        {
            _applicationId = Environment.GetEnvironmentVariable(Constants.ReefYarnApplicationIdEnvironmentVariable);
            _yarnConfiguration = YarnConfiguration.GetConfiguration();
        }

        public DriverInformation GetDriverInformation()
        {
            if (string.IsNullOrWhiteSpace(_applicationId))
            {
                throw new ApplicationException("Could not fetch the application ID from YARN's container environment variables.");
            }

            var yarnRMWebAppEndpoints = _yarnConfiguration.GetYarnRMWebappEndpoints();

            foreach (var yarnRMWebAppEndpoint in yarnRMWebAppEndpoints)
            {
                try
                {
                    var queryUri = new Uri(
                        yarnRMWebAppEndpoint, 
                        "proxy/" + _applicationId + "/" + Constants.HttpReefUriSpecification + Constants.HttpDriverUriTarget);
                    return DriverInformation.GetDriverInformationFromHttp(queryUri);
                }
                catch (Exception ex)
                {
                    Utilities.Diagnostics.Exceptions.Caught(
                        ex,
                        Level.Info,
                        "Unable to reach RM at " + yarnRMWebAppEndpoint,
                        Logger);
                }
            }

            throw new ApplicationException("Unable to get Driver Information.");
        }
    }
}