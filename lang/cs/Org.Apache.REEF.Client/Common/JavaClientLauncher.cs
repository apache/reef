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
using System.Threading.Tasks;
using Org.Apache.REEF.Client.API.Exceptions;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.Common
{
    /// <summary>
    /// Helper class to launch the java side of the various clients.
    /// </summary>
    internal class JavaClientLauncher : IJavaClientLauncher
    {
        /// <summary>
        /// The folder in which we search for the client jar.
        /// </summary>
        private const string JarFolder = "./";

        private static readonly Logger Logger = Logger.GetLogger(typeof(JavaClientLauncher));
        private readonly IList<string> _additionalClasspathEntries = new List<string>();

        [Inject]
        private JavaClientLauncher()
        {
        }

        /// <summary>
        /// Launch a java class in ClientConstants.ClientJarFilePrefix with provided parameters.
        /// </summary>
        /// <param name="javaLogLevel"></param>
        /// <param name="javaClassName"></param>
        /// <param name="parameters"></param>
        public Task LaunchAsync(JavaLoggingSetting javaLogLevel, string javaClassName, params string[] parameters)
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
            Logger.Log(Level.Info, msg);

            var process = Process.Start(startInfo);
            var processExitTracker = new TaskCompletionSource<bool>();
            if (process != null)
            {
                process.EnableRaisingEvents = true;
                process.OutputDataReceived += delegate(object sender, DataReceivedEventArgs e)
                {
                    if (!string.IsNullOrWhiteSpace(e.Data))
                    {
                        Logger.Log(Level.Info, e.Data);
                    }
                };
                process.ErrorDataReceived += delegate(object sender, DataReceivedEventArgs e)
                {
                    if (!string.IsNullOrWhiteSpace(e.Data))
                    {
                        Logger.Log(Level.Error, e.Data);
                    }
                };
                process.BeginErrorReadLine();
                process.BeginOutputReadLine();
                process.Exited += (sender, args) => { processExitTracker.SetResult(process.ExitCode == 0); };
            }
            else
            {
                processExitTracker.SetException(new Exception("Java client process didn't start."));
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
                Exceptions.Throw(
                    new JavaNotFoundException("JAVA_HOME isn't set. Please install Java and make set JAVA_HOME"), Logger);
            }

            if (!Directory.Exists(javaHomePath))
            {
                Exceptions.Throw(
                    new JavaNotFoundException("JAVA_HOME references a folder that doesn't exist.", javaHomePath), Logger);
            }

            var javaBinPath = Path.Combine(javaHomePath, "bin");
            if (!Directory.Exists(javaBinPath))
            {
                throw new JavaNotFoundException(
                    "JAVA_HOME references a folder that doesn't contain a `bin` folder. Please adjust JAVA_HOME",
                    javaHomePath);
            }

            var javaPath = Path.Combine(javaBinPath, "java.exe");
            if (!File.Exists(javaPath))
            {
                Exceptions.Throw(
                    new JavaNotFoundException(
                        "Could not find java.exe on this machine. Is Java installed and JAVA_HOME set?", javaPath),
                    Logger);
            }
            return javaPath;
        }

        /// <summary>
        /// Assembles the classpath for the side process
        /// </summary>
        /// <exception cref="ClasspathException">If the classpath would be empty.</exception>
        /// <returns></returns>
        private string GetClientClasspath()
        {
            var files = Directory.GetFiles(JarFolder)
                .Where(x => (!string.IsNullOrWhiteSpace(x)))
                .Where(e => Path.GetFileName(e).ToLower().StartsWith(ClientConstants.ClientJarFilePrefix))
                .ToList();

            if (files.Count == 0)
            {
                Exceptions.Throw(new ClasspathException(
                    "Unable to assemble classpath. Make sure the REEF JAR is in the current working directory."), Logger);
            }

            var classpathEntries = new List<string>(_additionalClasspathEntries).Concat(files);
            return string.Join(";", classpathEntries);
        }

        /// <summary>
        /// Add entries to the end of the classpath of the java client.
        /// </summary>
        /// <param name="entries"></param>
        public void AddToClassPath(IEnumerable<string> entries)
        {
            foreach (var entry in entries)
            {
                _additionalClasspathEntries.Add(entry);
            }
        }
    }
}