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

using System.Text;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.YARN.Parameters;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.YARN
{
    /// <summary>
    /// WindowsYarnJobCommandProvider is .NET implementation of `org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder`
    /// This class provides the command to be submitted to RM for execution of .NET driver running on Windows environment.
    /// </summary>
    internal sealed class WindowsYarnJobCommandProvider : IYarnJobCommandProvider
    {
        private static readonly string JavaExe = @"%JAVA_HOME%/bin/java";

        private static readonly string JvmOptionsPermSize = @"-XX:PermSize=128m";
        private static readonly string JvmOptionsMaxPermSizeFormat = @"-XX:MaxPermSize={0}m";
        private static readonly string JvmOptionsMaxMemoryAllocationPoolSizeFormat = @"-Xmx{0}m";
        private static readonly string ClassPathToken = @"-classpath";

        private static readonly string ProcReefProperty = @"-Dproc_reef";
        private static readonly string JavaLoggingProperty =
            @"-Djava.util.logging.config.class=org.apache.reef.util.logging.Config";
        private static readonly string LauncherClassName = @"org.apache.reef.bridge.client.YarnBootstrapREEFLauncher";
        private readonly REEFFileNames _fileNames;
        private readonly bool _enableDebugLogging;
        private readonly IYarnCommandLineEnvironment _yarnCommandLineEnvironment;
        private readonly int _driverMaxMemoryAllocationPoolSizeMB;
        private readonly int _driverMaxPermSizeMB;

        [Inject]
        private WindowsYarnJobCommandProvider(
            [Parameter(typeof(EnableDebugLogging))] bool enableDebugLogging,
            [Parameter(typeof(DriverMaxMemoryAllicationPoolSizeMB))] int driverMaxMemoryAllocationPoolSizeMB,
            [Parameter(typeof(DriverMaxPermSizeMB))] int driverMaxPermSizeMB,
            IYarnCommandLineEnvironment yarnCommandLineEnvironment,
            REEFFileNames fileNames)
        {
            _yarnCommandLineEnvironment = yarnCommandLineEnvironment;
            _enableDebugLogging = enableDebugLogging;
            _fileNames = fileNames;
            _driverMaxMemoryAllocationPoolSizeMB = driverMaxMemoryAllocationPoolSizeMB;
            _driverMaxPermSizeMB = driverMaxPermSizeMB;
        }

        /// <summary>
        /// Builds the command to be submitted to YARNRM
        /// </summary>
        /// <returns>Command string</returns>
        public string GetJobSubmissionCommand()
        {
            var sb = new StringBuilder();
            sb.Append(_fileNames.GetBridgeExePath());
            sb.Append(" " + JavaExe);
            sb.Append(" " + JvmOptionsPermSize);
            sb.Append(" " + string.Format(JvmOptionsMaxPermSizeFormat, _driverMaxPermSizeMB));
            sb.Append(" " +
                      string.Format(JvmOptionsMaxMemoryAllocationPoolSizeFormat, _driverMaxMemoryAllocationPoolSizeMB));
            sb.Append(" " + ClassPathToken);
            sb.Append(" " + string.Join(";", _yarnCommandLineEnvironment.GetYarnClasspathList()));
            sb.Append(" " + ProcReefProperty);
            if (_enableDebugLogging)
            {
                sb.Append(" " + JavaLoggingProperty);
            }

            sb.Append(" " + LauncherClassName);
            sb.Append(" " +
                      string.Format("{0}/{1}/{2}",
                          _fileNames.GetReefFolderName(),
                          _fileNames.GetLocalFolderName(),
                          _fileNames.GetJobSubmissionParametersFile()));
            sb.Append(" " + _fileNames.GetDriverLoggingConfigCommand());
            return sb.ToString();
        }
    }
}