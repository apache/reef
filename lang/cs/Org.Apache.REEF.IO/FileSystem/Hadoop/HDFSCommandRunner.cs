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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.IO.FileSystem.Hadoop.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IO.FileSystem.Hadoop
{
    /// <summary>
    /// Helper class to execute commands
    /// </summary>
    internal sealed class HdfsCommandRunner
    {
        /// <summary>
        /// The name of the <code>hdfs</code> command.
        /// </summary>
        private const string HdfsCommandName = "hdfs.cmd";

        /// <summary>
        /// The folder within <code>HadoopHome</code> that contains the <code>hdfs</code> command.
        /// </summary>
        private const string BinFolderName = "bin";

        /// <summary>
        /// The name of the HADOOP_HOME environment variable.
        /// </summary>
        private const string HadoopHomeEnvironmentVariableName = "HADOOP_HOME";

        private static readonly Logger Logger = Logger.GetLogger(typeof(HdfsCommandRunner));

        /// <summary>
        /// Path to hdfs.cmd
        /// </summary>
        private readonly string _hdfsCommandPath;

        /// <summary>
        /// The number of retries on HDFS commands.
        /// </summary>
        private readonly int _numberOfRetries;

        /// <summary>
        /// The timeout on each of the retries.
        /// </summary>
        private readonly int _timeOutInMilliSeconds;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="numberOfRetries"></param>
        /// <param name="timeOutInMilliSeconds"></param>
        /// <param name="hadoopHome"></param>
        /// <exception cref="FileNotFoundException">If the hdfs command can't be found.</exception>
        [Inject]
        private HdfsCommandRunner([Parameter(typeof(NumberOfRetries))] int numberOfRetries,
            [Parameter(typeof(CommandTimeOut))] int timeOutInMilliSeconds,
            [Parameter(typeof(HadoopHome))] string hadoopHome)
        {
            _numberOfRetries = numberOfRetries;
            _timeOutInMilliSeconds = timeOutInMilliSeconds;

            if (!PathUtilities.AreNormalizedEquals(hadoopHome, HadoopHome.DefaultValue))
            {
                // The user provided a Hadoop Home folder. 
                if (Directory.Exists(hadoopHome))
                {
                    // The user provided folder does exist.
                    _hdfsCommandPath = GetFullPathToHdfsCommand(hadoopHome);
                }
                else
                {
                    // The user provided folder does not exist. Try the environment variable.
                    Logger.Log(Level.Warning,
                        "The provided hadoop home folder {0} doesn't exist, trying environment variable {1} instead",
                        hadoopHome, HadoopHomeEnvironmentVariableName);
                    _hdfsCommandPath = GetFullPathToHdfsCommandBasedOnEnvironmentVariable();
                }
            }
            else
            {
                // The user did not provide a Hadoop Home folder. Use the Environment variable.
                _hdfsCommandPath = GetFullPathToHdfsCommandBasedOnEnvironmentVariable();
            }

            // Make sure we found the command.
            if (!File.Exists(_hdfsCommandPath))
            {
                throw new FileNotFoundException("HDFS Command not found", _hdfsCommandPath);
            }
        }

        internal CommandResult Run(string hdfsCommandLineArguments)
        {
            var processStartInfo = new ProcessStartInfo
            {
                FileName = _hdfsCommandPath,
                Arguments = hdfsCommandLineArguments,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };
            for (var attemptNumber = 0; attemptNumber < _numberOfRetries; ++attemptNumber)
            {
                var processName = string.Format("HDFS_Attempt_{0}_of_{1}", attemptNumber, _numberOfRetries);
                var result = RunAttempt(processStartInfo, _timeOutInMilliSeconds, processName);
                if (null != result)
                {
                    LogCommandOutput(result);
                    return result;
                }
            }

            // If we reached here, we ran out of retries.
            throw new Exception(
                string.Format("HDFS Cmd {0} {1} could not be executed in the specified timeout & retry settings",
                    _hdfsCommandPath, hdfsCommandLineArguments));
        }

        /// <summary>
        /// Utility method that constructs the full absolute path to the <code>hdfs</code> command.
        /// </summary>
        /// <param name="hadoopHome"></param>
        /// <returns></returns>
        private static string GetFullPathToHdfsCommand(string hadoopHome)
        {
            return Path.Combine(Path.GetFullPath(hadoopHome), BinFolderName, HdfsCommandName);
        }

        /// <summary>
        /// Constructs the path to the HDFS binary based on the HADOOP_HOME environment variable.
        /// </summary>
        /// <returns></returns>
        private static string GetFullPathToHdfsCommandBasedOnEnvironmentVariable()
        {
            var hadoopHomeFromEnv = Environment.GetEnvironmentVariable(HadoopHomeEnvironmentVariableName);
            Logger.Log(Level.Verbose, "{0} evaluated to {1}.", HadoopHomeEnvironmentVariableName, hadoopHomeFromEnv);
            if (null == hadoopHomeFromEnv)
            {
                throw new Exception(HadoopHomeEnvironmentVariableName +
                                    " not set and no path to the hadoop installation provided.");
            }
            return GetFullPathToHdfsCommand(hadoopHomeFromEnv);
        }

        /// <summary>
        /// Helper method to log the command result.
        /// </summary>
        /// <param name="result"></param>
        private static void LogCommandOutput(CommandResult result)
        {
            using (var messageBuilder = new StringWriter())
            {
                messageBuilder.WriteLine("OUTPUT:");
                messageBuilder.WriteLine("----------------------------------------");
                foreach (var stdOut in result.StdOut)
                {
                    messageBuilder.WriteLine("Out:    " + stdOut);
                }

                messageBuilder.WriteLine("----------------------------------------");
                foreach (var stdErr in result.StdErr)
                {
                    messageBuilder.WriteLine("Err:    " + stdErr);
                }
                messageBuilder.WriteLine("----------------------------------------");
                Logger.Log(Level.Verbose, messageBuilder.ToString());
            }
        }

        /// <summary>
        /// Attempts to run a process with a timeout.
        /// </summary>
        /// <returns>The result of the attempt or null in case of timeout.</returns>
        /// <param name="processStartInfo">The process start information.</param>
        /// <param name="timeOutInMilliSeconds">Timeout for the process.</param>
        /// <param name="processName">A human readable name used for logging purposes.</param>
        private static CommandResult RunAttempt(ProcessStartInfo processStartInfo, int timeOutInMilliSeconds,
            string processName)
        {
            // Setup the process.
            var outList = new List<string>();
            var errList = new List<string>();
            processStartInfo.RedirectStandardError = true;
            processStartInfo.RedirectStandardOutput = true;
            var process = new Process
            {
                StartInfo = processStartInfo
            };

            process.OutputDataReceived += delegate(object sender, DataReceivedEventArgs e)
            {
                if (!string.IsNullOrWhiteSpace(e.Data))
                {
                    outList.Add(e.Data.Trim());
                }
            };

            process.ErrorDataReceived += delegate(object sender, DataReceivedEventArgs e)
            {
                if (!string.IsNullOrWhiteSpace(e.Data))
                {
                    errList.Add(e.Data.Trim());
                }
            };

            // Start it
            process.Start();
            process.BeginErrorReadLine();
            process.BeginOutputReadLine();
            Logger.Log(Level.Verbose, "Waiting for {0}ms for process `{1}` to finish", timeOutInMilliSeconds,
                processName);

            // Deal with timeouts
            process.WaitForExit(timeOutInMilliSeconds);

            if (process.HasExited)
            {
                // The happy path: Assemble an output
                return new CommandResult(outList, errList, process.ExitCode);
            }

            // If we didn't return above, the process timed out.
            Logger.Log(Level.Info, "The process `{0}` took longer than {1}ms to exit. Killing it.", processName,
                timeOutInMilliSeconds);
            process.Kill();
            process.WaitForExit();
            Logger.Log(Level.Info, "Killed process `{0}`.", processName);
            return null;
        }
    }
}