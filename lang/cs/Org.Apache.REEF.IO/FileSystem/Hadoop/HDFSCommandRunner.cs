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

        [Inject]
        private HdfsCommandRunner([Parameter(typeof(NumberOfRetries))] int numberOfRetries,
            [Parameter(typeof(CommandTimeOut))] int timeOutInMilliSeconds,
            [Parameter(typeof(HadoopHome))] string hadoopHome)
        {
            _numberOfRetries = numberOfRetries;
            _timeOutInMilliSeconds = timeOutInMilliSeconds;

            if (!HadoopHome.DefaultValue.Equals(hadoopHome))
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
                    Logger.Log(Level.Warning, "The provided hadoop home folder {0} doesn't exist, trying environment variable {1} instead",
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
                throw new Exception("HDFS command does not exist: " + _hdfsCommandPath);
            }
        }

        internal CommandResult Run(string hdfsCommandLineArguments)
        {
            var outList = new List<string>();
            var errList = new List<string>();
            var tries = _numberOfRetries;
            var origTries = _numberOfRetries;
            Process process;
            do
            {
                outList.Clear();
                Logger.Log(Level.Info, "Trial {0} Timeout in {1} secs Executing: {2}", origTries - tries,
                    _timeOutInMilliSeconds/1000,
                    _hdfsCommandPath + " " + hdfsCommandLineArguments);
                var startInfo = new ProcessStartInfo
                {
                    FileName = _hdfsCommandPath,
                    Arguments = hdfsCommandLineArguments,
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true
                };

                process = Process.Start(startInfo);

                if (process == null)
                {
                    throw new Exception(string.Format("HDFS cmd {0} {1} process didn't start.", _hdfsCommandPath,
                        hdfsCommandLineArguments));
                }

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
                process.BeginErrorReadLine();
                process.BeginOutputReadLine();

                if (process.WaitForExit(_timeOutInMilliSeconds))
                {
                    break;
                }

                Logger.Log(Level.Warning, "Process has not exited after a timeout of {0} secs. So killing it",
                    _timeOutInMilliSeconds);
                process.Kill();
            } while (--tries > 0);

            if (!process.HasExited)
            {
                process.Kill();
                throw new Exception(
                    string.Format("HDFS Cmd {0} {1} could not be executed in the specified timeout & retry settings",
                        _hdfsCommandPath, hdfsCommandLineArguments));
            }
            process.WaitForExit();

            #region CommandOutputLogging

            if (Logger.IsLoggable(Level.Verbose))
            {
                using (var messageBuilder = new StringWriter())
                {
                    messageBuilder.WriteLine("OUTPUT:");
                    messageBuilder.WriteLine("----------------------------------------");
                    foreach (var stdOut in outList)
                    {
                        messageBuilder.WriteLine("Out:    " + stdOut);
                    }

                    messageBuilder.WriteLine("----------------------------------------");
                    foreach (var stdErr in errList)
                    {
                        messageBuilder.WriteLine("Err:    " + stdErr);
                    }
                    messageBuilder.WriteLine("----------------------------------------");
                    Logger.Log(Level.Verbose, messageBuilder.ToString());
                }
            }

            #endregion

            return new CommandResult(outList, errList, process.ExitCode);
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
    }
}