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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Client.API.Exceptions;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Bridge.Core.Common.Client
{
    /// <summary>
    /// Helper class to launch the java side of the various clients.
    /// </summary>
    internal sealed class JavaClientLauncher 
    {
        /// <summary>
        /// The folder in which we search for the client jar.
        /// </summary>
        private const string JarFolder = "./";

        private static readonly Logger Log = Logger.GetLogger(typeof(JavaClientLauncher));

        [Inject]
        private JavaClientLauncher()
        {
        }

        /// <summary>
        /// Launch a java class in ClientConstants.ClientJarFilePrefix with provided parameters.
        /// </summary>
        /// <param name="javaLogLevel">Java logging level</param>
        /// <param name="javaClassName">Java class that launches the Java driver</param>
        /// <param name="parameters">Parameters associated with the Java main class</param>
        /// <param name="cancellationToken">Token to cancel the launch</param>
        public Task LaunchAsync(
            JavaLoggingSetting javaLogLevel, 
            string javaClassName, 
            string[] parameters, 
            CancellationToken cancellationToken = default)
        {
            var startInfo = new ProcessStartInfo
            {
                Arguments = AssembleArguments(javaLogLevel, javaClassName, parameters),
                FileName = GetJavaCommand(),
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,

            };
            var msg = string.Format(CultureInfo.CurrentCulture, "Launch Java with command: {0} {1}",
                startInfo.FileName, startInfo.Arguments);
            Log.Log(Level.Info, msg);

            var processExitTracker = new TaskCompletionSource<bool>();
            var process = new Process
            {
                StartInfo = startInfo,
                EnableRaisingEvents = true
            };
            process.Exited += (sender, args) => { processExitTracker.SetResult(process.ExitCode == 0); };
            if (cancellationToken != default)
            {
                cancellationToken.Register(processExitTracker.SetCanceled);
            }
            process.OutputDataReceived += delegate(object sender, DataReceivedEventArgs e)
            {
                if (!string.IsNullOrWhiteSpace(e.Data))
                {
                    Log.Log(Level.Info, e.Data);
                }
            };
            process.ErrorDataReceived += delegate(object sender, DataReceivedEventArgs e)
            {
                if (!string.IsNullOrWhiteSpace(e.Data))
                {
                    Log.Log(Level.Error, e.Data);
                }
            };
            if (!process.Start())
            {
                processExitTracker.SetException(new Exception("Java client process didn't start."));
            }
            else
            {
                process.BeginErrorReadLine();
                process.BeginOutputReadLine();
            }
            return processExitTracker.Task;
        }

        /// <summary>
        /// Assembles the command line arguments. Used by LaunchAsync()
        /// </summary>
        /// <param name="javaLogLevel"></param>
        /// <param name="javaClassName"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        private string AssembleArguments(JavaLoggingSetting javaLogLevel, string javaClassName, params string[] parameters)
        {
            IList<string> arguments = new List<string>();

            if (javaLogLevel == JavaLoggingSetting.Verbose)
            {
                arguments.Add("-Djava.util.logging.config.class=org.apache.reef.util.logging.Config");
            }

            arguments.Add("-cp");
            arguments.Add(GetClientClasspath());
            arguments.Add(javaClassName);
            foreach (var parameter in parameters)
            {
                arguments.Add(parameter);
            }
            return string.Join(" ", arguments);
        }

        /// <summary>
        /// Find the `java` command on this machine and returns its path.
        /// </summary>
        /// <exception cref="JavaNotFoundException">If the java binary couldn't be found.</exception>
        /// <returns>The path of the `java` command on this machine.</returns>
        private static string GetJavaCommand()
        {
            var javaHomePath = Environment.GetEnvironmentVariable("JAVA_HOME");
            if (string.IsNullOrWhiteSpace(javaHomePath))
            {
                // TODO: Attempt to find java via the registry.
                throw new IllegalStateException("JAVA_HOME isn't set. Please install Java and make set JAVA_HOME");
            }

            if (!Directory.Exists(javaHomePath))
            {
                throw new IllegalStateException($"JAVA_HOME references a folder that doesn\'t exist. {javaHomePath}");
            }

            var javaBinPath = Path.Combine(javaHomePath, "bin");
            if (!Directory.Exists(javaBinPath))
            {
                throw new IllegalStateException(
                    $"JAVA_HOME references a folder that doesn\'t contain a `bin` folder. Please adjust JAVA_HOME {javaHomePath}");
            }

            var javaPath = Path.Combine(javaBinPath, "java.exe");
            if (File.Exists(javaPath)) return javaPath;
            javaPath = Path.Combine(javaBinPath, "java");
            if (!File.Exists(javaPath))
            {
                throw new IllegalStateException(
                    "Could not find java executable on this machine. Is Java installed and JAVA_HOME set? " + javaBinPath);
            }
            return javaPath;
        }

        /// <summary>
        /// Assembles the classpath for the side process
        /// </summary>
        /// <exception cref="ClasspathException">If the classpath would be empty.</exception>
        /// <returns></returns>
        private static string GetClientClasspath()
        {
            var files = Directory.GetFiles(JarFolder)
                .Where(x => (!string.IsNullOrWhiteSpace(x)))
                .Where(e => Path.GetFileName(e).ToLower().Contains("reef-bridge-proto-java"))
                .ToList();
            if (files.Count == 0)
            {
                throw new IllegalStateException(
                    "Unable to assemble classpath. Make sure the REEF Jar is in the current working directory.");
            }
            return string.Join(";", files);
        }
    }
}