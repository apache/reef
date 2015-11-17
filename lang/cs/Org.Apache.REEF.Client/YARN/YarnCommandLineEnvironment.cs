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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.Yarn
{
    /// <summary>
    /// Helper class to interact with the YARN command line.
    /// </summary>
    internal sealed class YarnCommandLineEnvironment : IYarnCommandLineEnvironment
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(YarnCommandLineEnvironment));

        [Inject]
        private YarnCommandLineEnvironment()
        {
        }

        /// <summary>
        /// Returns the class path returned by `yarn classpath`.
        /// </summary>
        /// <returns>The class path returned by `yarn classpath`.</returns>
        public IList<string> GetYarnClasspathList()
        {
            return Yarn("classpath").Split(';').Distinct().ToList();
        }

        /// <summary>
        /// Returns the full Path to HADOOP_HOME
        /// </summary>
        /// <returns>The full Path to HADOOP_HOME</returns>
        private string GetHadoopHomePath()
        {
            var path = Environment.GetEnvironmentVariable("HADOOP_HOME");
            if (string.IsNullOrWhiteSpace(path))
            {
                var ex = new FileNotFoundException("HADOOP_HOME isn't set.");
                Exceptions.Throw(ex, Logger);
                throw ex;
            }

            var fullPath = Path.GetFullPath(path);

            if (!Directory.Exists(fullPath))
            {
                var ex = new FileNotFoundException("HADOOP_HOME points to [" + fullPath + "] which doesn't exist.");
                Exceptions.Throw(ex, Logger);
                throw ex;
            }
            return fullPath;
        }

        /// <summary>
        /// Returns the full Path to the `yarn.cmd` file.
        /// </summary>
        /// <returns>The full Path to the `yarn.cmd` file.</returns>
        private string GetYarnCommandPath()
        {
            var result = Path.Combine(GetHadoopHomePath(), "bin", "yarn.cmd");
            if (!File.Exists(result))
            {
                var ex = new FileNotFoundException("Couldn't find yarn.cmd", result);
                Exceptions.Throw(ex, Logger);
                throw ex;
            }
            return result;
        }

        /// <summary>
        /// Executes `yarn.cmd` with the given parameters.
        /// </summary>
        /// <param name="arguments"></param>
        /// <returns>Whatever was printed to stdout by YARN.</returns>
        private string Yarn(params string[] arguments)
        {
            var startInfo = new ProcessStartInfo
            {
                FileName = GetYarnCommandPath(),
                Arguments = string.Join(" ", arguments),
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = false
            };
            var process = Process.Start(startInfo);
            var output = new StringBuilder();
            if (process != null)
            {
                process.OutputDataReceived += delegate(object sender, DataReceivedEventArgs e)
                {
                    if (!string.IsNullOrWhiteSpace(e.Data))
                    {
                        output.Append(e.Data);
                    }
                };
                process.BeginOutputReadLine();
                process.WaitForExit();
            }
            else
            {
                var ex = new Exception("YARN process didn't start.");
                Exceptions.Throw(ex, Logger);
                throw ex;
            }
            return output.ToString();
        }
    }
}