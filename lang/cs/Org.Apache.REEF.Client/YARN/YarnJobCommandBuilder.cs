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

using System.Collections.Generic;
using System.Text;
using Org.Apache.REEF.Client.YARN.Parameters;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.YARN
{
    /// <summary>
    /// YarnJobCommandBuilder is .NET implementation of `org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder`
    /// This class provides the command to be submitted to RM for execution of .NET driver.
    /// </summary>
    public sealed class YarnJobCommandBuilder : IYarnJobCommandBuilder
    {
        private static readonly string JavaExe = @"%JAVA_HOME%/bin/java";

        // keeping static values for now; make dynamic when needed
        private static readonly string JvmOptions = @"-XX:PermSize=128m -XX:MaxPermSize=128m -Xmx512m";
        private static readonly string ClassPathToken = @"-classpath";

        private static readonly List<string> ClassPathItems = new List<string>
        {
            "%HADOOP_CONF_DIR%",
            "%HADOOP_HOME%/*",
            "%HADOOP_HOME%/lib/*",
            "%HADOOP_COMMON_HOME%/*",
            "%HADOOP_COMMON_HOME%/lib/*",
            "%HADOOP_YARN_HOME%/*",
            "%HADOOP_YARN_HOME%/lib/*",
            "%HADOOP_HDFS_HOME%/*",
            "%HADOOP_HDFS_HOME%/lib/*",
            "%HADOOP_MAPRED_HOME%/*",
            "%HADOOP_MAPRED_HOME%/lib/*",
            "%HADOOP_HOME%/etc/hadoop",
            "%HADOOP_HOME%/share/hadoop/common/*",
            "%HADOOP_HOME%/share/hadoop/common/lib/*",
            "%HADOOP_HOME%/share/hadoop/yarn/*",
            "%HADOOP_HOME%/share/hadoop/yarn/lib/*",
            "%HADOOP_HOME%/share/hadoop/hdfs/*",
            "%HADOOP_HOME%/share/hadoop/hdfs/lib/*",
            "%HADOOP_HOME%/share/hadoop/mapreduce/*",
            "%HADOOP_HOME%/share/hadoop/mapreduce/lib/*",
            "reef/local/*",
            "reef/global/*"
        };

        private static readonly string ProcReefProperty = @"-Dproc_reef";
        private static readonly string LoggingProperty =
            @"-Djava.util.logging.config.class=org.apache.reef.util.logging.Config";
        private static readonly string LauncherClassName = @"org.apache.reef.bridge.client.YarnBootstrapREEFLauncher";
        private static readonly string ParametersFilePath = @"reef/local/yarnparameters.json";
        private static readonly string DriverLoggingConfig = "1> <LOG_DIR>/driver.stdout 2> <LOG_DIR>/driver.stderr";
        private readonly REEFFileNames _fileNames;
        private readonly bool _enableDebugLogging;


        [Inject]
        private YarnJobCommandBuilder(
            [Parameter(typeof(EnableDebugLogging))] bool enableDebugLogging,
            REEFFileNames fileNames)
        {
            _enableDebugLogging = enableDebugLogging;
            _fileNames = fileNames;
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
            sb.Append(" " + JvmOptions);
            sb.Append(" " + ClassPathToken);
            sb.Append(" " + string.Join(";", ClassPathItems));
            sb.Append(" " + ProcReefProperty);
            if (_enableDebugLogging)
            {
                sb.Append(" " + LoggingProperty);
            }

            sb.Append(" " + LauncherClassName);
            sb.Append(" " + ParametersFilePath);
            sb.Append(" " + DriverLoggingConfig);
            return sb.ToString();
        }
    }
}