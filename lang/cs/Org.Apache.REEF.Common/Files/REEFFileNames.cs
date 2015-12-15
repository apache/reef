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

using System.Diagnostics.CodeAnalysis;
using System.IO;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Files
{
    /// <summary>
    /// Contains all file system constants used by REEF.
    /// </summary>
    /// <remarks>
    /// Whenever editing this, make sure you also edit org.apache.reef.runtime.common.files.REEFFileNames in the java
    /// code base.
    /// </remarks>
    [SuppressMessage("ReSharper", "InconsistentNaming",
        Justification = "The names are all taken from the Java codebase.")]
    public sealed class REEFFileNames
    {
        private const string JAR_FILE_SUFFIX = ".jar";
        private const string JOB_FOLDER_PREFIX = "reef-job-";
        private const string EVALUATOR_FOLDER_PREFIX = "reef-evaluator-";
        private const string DRIVER_STDERR = "driver.stderr";
        private const string DRIVER_STDOUT = "driver.stdout";
        private const string EVALUATOR_STDERR = "evaluator.stderr";
        private const string EVALUATOR_STDOUT = "evaluator.stdout";
        private const string CPP_BRIDGE = "JavaClrBridge";
        private const string REEF_BASE_FOLDER = "reef";
        private const string GLOBAL_FOLDER = "global";
        private const string LOCAL_FOLDER = "local";
        private const string DRIVER_CONFIGURATION_NAME = "driver.conf";
        private const string EVALUATOR_CONFIGURATION_NAME = "evaluator.conf";
        private const string CLR_DRIVER_CONFIGURATION_NAME = "clrdriver.conf";
        private const string DRIVER_HTTP_ENDPOINT_FILE_NAME = "DriverHttpEndpoint.txt";
        private const string BRIDGE_EXE_NAME = "Org.Apache.REEF.Bridge.exe";
        private const string BRIDGE_EXE_CONFIG_NAME = "Org.Apache.REEF.Bridge.exe.config";
        private const string SECURITY_TOKEN_IDENTIFIER_FILE = "SecurityTokenId";
        private const string SECURITY_TOKEN_PASSWORD_FILE = "SecurityTokenPwd";
        private const string JOB_SUBMISSION_PARAMETERS_FILE = "job-submission-params.json";
        private const string DRIVER_COMMAND_LOGGING_CONFIG = "1> <LOG_DIR>/driver.stdout 2> <LOG_DIR>/driver.stderr";

        [Inject]
        public REEFFileNames()
        {
        }

        /// <summary>
        /// The name of the REEF folder inside of the working directory of an Evaluator or Driver
        /// </summary>
        /// <returns></returns>
        public string GetReefFolderName()
        {
            return REEF_BASE_FOLDER;
        }

        /// <summary>
        /// </summary>
        /// <returns>the name of the folder inside of REEF_BASE_FOLDER that houses the global files.</returns>
        public string GetGlobalFolderName()
        {
            return GLOBAL_FOLDER;
        }

        /// <summary>
        /// </summary>
        /// <returns>the path to the global folder: REEF_BASE_FOLDER/GLOBAL_FOLDER</returns>
        public string GetGlobalFolderPath()
        {
            return GLOBAL_FOLDER_PATH;
        }

        /// <summary>
        /// </summary>
        /// <returns>the name of the folder inside of REEF_BASE_FOLDER that houses the local files.</returns>
        public string GetLocalFolderName()
        {
            return LOCAL_FOLDER;
        }

        /// <summary>
        /// </summary>
        /// <returns>the path to the local folder: REEF_BASE_FOLDER/LOCAL_FOLDER</returns>
        public string GetLocalFolderPath()
        {
            return LOCAL_FOLDER_PATH;
        }

        /// <summary>
        /// </summary>
        /// <returns>The name under which the driver configuration will be stored in REEF_BASE_FOLDER/LOCAL_FOLDER</returns>
        public string GetDriverConfigurationName()
        {
            return DRIVER_CONFIGURATION_NAME;
        }

        /// <summary>
        /// </summary>
        /// <returns>The name under which the driver configuration will be stored in REEF_BASE_FOLDER/LOCAL_FOLDER</returns>
        public string GetClrDriverConfigurationName()
        {
            return CLR_DRIVER_CONFIGURATION_NAME;
        }

        /// <summary>
        /// </summary>
        /// <returns>The path to the driver configuration: GLOBAL_FOLDER/LOCAL_FOLDER/DRIVER_CONFIGURATION_NAME</returns>
        public string GetDriverConfigurationPath()
        {
            return DRIVER_CONFIGURATION_PATH;
        }

        /// <summary>
        /// </summary>
        /// <returns>The path to the CLR driver configuration: GLOBAL_FOLDER/LOCAL_FOLDER/CLR_DRIVER_CONFIGURATION_NAME</returns>
        public string GetClrDriverConfigurationPath()
        {
            return CLR_DRIVER_CONFIGURATION_PATH;
        }

        /// <summary>
        /// </summary>
        /// <returns>REEF_BASE_FOLDER/LOCAL_FOLDER</returns>
        public string GetEvaluatorConfigurationName()
        {
            return EVALUATOR_CONFIGURATION_NAME;
        }

        /// <summary>
        /// </summary>
        /// <returns>the path to the evaluator configuration.</returns>
        public string GetEvaluatorConfigurationPath()
        {
            return EVALUATOR_CONFIGURATION_PATH;
        }

        /// <summary>
        /// </summary>
        /// <returns> The suffix used for JAR files, including the "."</returns>
        public string GetJarFileSuffix()
        {
            return JAR_FILE_SUFFIX;
        }

        /// <summary>
        /// The prefix used whenever REEF is asked to create a job folder, on (H)DFS or locally. This prefix is also used with
        /// JAR files created to represent a job.
        /// </summary>
        /// <returns></returns>
        public string GetJobFolderPrefix()
        {
            return JOB_FOLDER_PREFIX;
        }

        /// <summary>
        /// </summary>
        /// <returns>The name used within the current working directory of the driver to redirect standard error to.</returns>
        public string GetDriverStderrFileName()
        {
            return DRIVER_STDERR;
        }

        /// <summary>
        /// </summary>
        /// <returns>The name used within the current working directory of the driver to redirect standard out to.</returns>
        public string GetDriverStdoutFileName()
        {
            return DRIVER_STDOUT;
        }

        /// <summary>
        /// </summary>
        /// <returns>The prefix used whenever REEF is asked to create an Evaluator folder, e.g. for staging.</returns>
        public string GetEvaluatorFolderPrefix()
        {
            return EVALUATOR_FOLDER_PREFIX;
        }

        /// <summary>
        /// </summary>
        /// <returns>The name used within the current working directory of the driver to redirect standard error to.</returns>
        public string GetEvaluatorStderrFileName()
        {
            return EVALUATOR_STDERR;
        }

        /// <summary>
        /// </summary>
        /// <returns>The name used within the current working directory of the driver to redirect standard out to.</returns>
        public string GetEvaluatorStdoutFileName()
        {
            return EVALUATOR_STDOUT;
        }

        /// <summary>
        /// </summary>
        /// <returns>the name of cpp bridge file</returns>
        public string GetCppBridge()
        {
            return CPP_BRIDGE;
        }

        /// <summary>
        /// The path of the Driver Launcher exe.
        /// </summary>
        /// <returns>path of the Driver Launcher EXE.</returns>
        public string GetBridgeExePath()
        {
            return Path.Combine(REEF_BASE_FOLDER, BRIDGE_EXE_NAME);
        }

        /// <summary>
        /// The path of the Driver Launcher exe config .
        /// </summary>
        /// <returns>path of the Driver Launcher exe config.</returns>
        public string GetBridgeExeConfigPath()
        {
            return Path.Combine(REEF_BASE_FOLDER, BRIDGE_EXE_CONFIG_NAME);
        }

        /// <summary>
        /// The Job Submission parameters file that is used to submit a job through Java,
        /// either directly or via a "bootstrap" method.
        /// </summary>
        public string GetJobSubmissionParametersFile()
        {
            return JOB_SUBMISSION_PARAMETERS_FILE;
        }

        /// <summary>
        /// The command that allows redirecting Driver stdout and stderr logs
        /// to appropriate files
        /// </summary>
        /// <returns></returns>
        public string GetDriverLoggingConfigCommand()
        {
            return DRIVER_COMMAND_LOGGING_CONFIG;
        }

        /// <summary>
        /// The filename for security token identifier
        /// </summary>
        /// <returns>filename which contains raw bytes of security token identifier</returns>
        [Unstable("0.13", "Security token should be handled by .NET only REEF client in the future")]
        public string GetSecurityTokenIdentifierFileName()
        {
            return SECURITY_TOKEN_IDENTIFIER_FILE;
        }

        /// <summary>
        /// The filename for security token password
        /// </summary>
        /// <returns>filename which contains raw bytes of security token password</returns>
        [Unstable("0.13", "Security token should be handled by .NET only REEF client in the future")]
        public string GetSecurityTokenPasswordFileName()
        {
            return SECURITY_TOKEN_PASSWORD_FILE;
        }

        /// <summary>
        /// </summary>
        /// <returns>File name that contains the dfs path for the DriverHttpEndpoint</returns>
        [Unstable("0.13", "Working in progress for what to return after submit")]
        public string DriverHttpEndpoint 
        { 
            get { return DRIVER_HTTP_ENDPOINT_FILE_NAME; } 
        }

        private static readonly string GLOBAL_FOLDER_PATH = Path.Combine(REEF_BASE_FOLDER, GLOBAL_FOLDER);
        private static readonly string LOCAL_FOLDER_PATH = Path.Combine(REEF_BASE_FOLDER, LOCAL_FOLDER);

        private static readonly string DRIVER_CONFIGURATION_PATH = Path.Combine(LOCAL_FOLDER_PATH,
            DRIVER_CONFIGURATION_NAME);

        private static readonly string CLR_DRIVER_CONFIGURATION_PATH = Path.Combine(LOCAL_FOLDER_PATH,
            CLR_DRIVER_CONFIGURATION_NAME);

        private static readonly string EVALUATOR_CONFIGURATION_PATH =
            Path.Combine(LOCAL_FOLDER_PATH, EVALUATOR_CONFIGURATION_NAME);
    }
}