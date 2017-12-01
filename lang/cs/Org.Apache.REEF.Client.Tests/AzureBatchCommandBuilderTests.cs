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

using Org.Apache.REEF.Client.AzureBatch.Util;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Xunit;

namespace Org.Apache.REEF.Client.Tests
{
    public class AzureBatchCommandBuilder
    {
        [Fact]
        public void WindowsCommandBuilderDriverTest()
        {
            // Prepare
            const int driverMemory = 100;
            AbstractCommandBuilder builder = TestContext.GetWindowsCommandBuilder();
            string expected = "powershell.exe /c \"Add-Type -AssemblyName System.IO.Compression.FileSystem; " +
                            "[System.IO.Compression.ZipFile]::ExtractToDirectory(\\\"$env:AZ_BATCH_TASK_WORKING_DIR\\local.jar\\\", " +
                            "\\\"$env:AZ_BATCH_TASK_WORKING_DIR\\reef\\\");reef\\Org.Apache.REEF.Bridge.exe " +
                            "java -Xmx100m -XX:PermSize=128m -XX:MaxPermSize=128m -classpath 'reef/local/*;reef/global/*;' " +
                            "-Dproc_reef org.apache.reef.bridge.client.AzureBatchBootstrapREEFLauncher " +
                            "reef\\job-submission-params.json\";";

            // Action
            string actual = builder.BuildDriverCommand(driverMemory);

            // Assert
            Assert.Equal(expected, actual);
        }

        private class TestContext
        {
            public static AbstractCommandBuilder GetWindowsCommandBuilder()
            {
                IInjector injector = TangFactory.GetTang().NewInjector();
                return injector.GetInstance<WindowsCommandBuilder>();
            }
        }
    }
}
