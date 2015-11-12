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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Driver;

namespace Org.Apache.REEF.Tests.Utility
{
    [TestClass]
    public class TestDriverConfigGenerator
    {
        [TestMethod]
        public void TestGeneratingFullDriverConfigFile()
        {
            DriverConfigurationSettings driverSubmissionSettings = new DriverConfigurationSettings()
            {
                DriverMemory = 1024,
                DriverIdentifier = "juliaDriverId",
                SubmissionDirectory = "reefClrBridgeTmp/job_" + Guid.NewGuid().ToString("N").Substring(0, 4),
                IncludingHttpServer = true,
                IncludingNameServer = true,
                ClrFolder = ".",
                JarFileFolder = ".\\"
            };

            DriverConfigGenerator.DriverConfigurationBuilder(driverSubmissionSettings);
        }

        [TestMethod]
        public void TestGeneratingDriverConfigFileWithoutHttp()
        {
            DriverConfigurationSettings driverSubmissionSettings = new DriverConfigurationSettings()
            {
                DriverMemory = 1024,
                DriverIdentifier = "juliaDriverId",
                SubmissionDirectory = "reefClrBridgeTmp/job_" + Guid.NewGuid().ToString("N").Substring(0, 4),
                IncludingHttpServer = false,
                IncludingNameServer = true,
                ClrFolder = ".",
                JarFileFolder = ".\\"
            };

            DriverConfigGenerator.DriverConfigurationBuilder(driverSubmissionSettings);
        }

        [TestMethod]
        public void TestGeneratingDriverConfigFileWithoutNameServer()
        {
            DriverConfigurationSettings driverSubmissionSettings = new DriverConfigurationSettings()
            {
                DriverMemory = 1024,
                DriverIdentifier = "juliaDriverId",
                SubmissionDirectory = "reefClrBridgeTmp/job_" + Guid.NewGuid().ToString("N").Substring(0, 4),
                IncludingHttpServer = true,
                IncludingNameServer = false,
                ClrFolder = ".",
                JarFileFolder = ".\\"
            };

            DriverConfigGenerator.DriverConfigurationBuilder(driverSubmissionSettings);
        }

        [TestMethod]
        public void TestGeneratingDriverConfigFileDriverOnly()
        {
            DriverConfigurationSettings driverSubmissionSettings = new DriverConfigurationSettings()
            {
                DriverMemory = 1024,
                DriverIdentifier = "juliaDriverId",
                SubmissionDirectory = "reefClrBridgeTmp/job_" + Guid.NewGuid().ToString("N").Substring(0, 4),
                IncludingHttpServer = false,
                IncludingNameServer = false,
                ClrFolder = ".",
                JarFileFolder = ".\\"
            };

            DriverConfigGenerator.DriverConfigurationBuilder(driverSubmissionSettings);
        }
    }
}
