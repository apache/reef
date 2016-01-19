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
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

[module: SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1402:FileMayOnlyContainASingleClass", Justification = "allow name parameter class to be embedded")]

namespace Org.Apache.REEF.Examples.Tasks.ShellTask
{
    public class ShellTask : ITask
    {
        [Inject]
        public ShellTask([Parameter(Value = typeof(Command))] string command)
        {
            Cmd = command;
        }

        public string Cmd { get; set; }

        public byte[] Call(byte[] memento)
        {
            return Encoding.UTF8.GetBytes(CommandLineExecute(Cmd));
        }

        public void Dispose()
        {
        }

        private string CommandLineExecute(string command)
        {
            string output = string.Empty;
            try
            {
                ProcessStartInfo startInfo = new ProcessStartInfo()
                {
                    FileName = "cmd.exe",
                    Arguments = @"/c " + command,
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };

                using (Process process = Process.Start(startInfo))
                {
                    StringBuilder standardOutput = new StringBuilder();

                    process.WaitForExit(1000);

                    standardOutput.Append(process.StandardOutput.ReadToEnd());
                    output = standardOutput.ToString();
                }
            }
            catch (Exception e)
            {
                output = string.Format(CultureInfo.InvariantCulture, "Failed to execute command [{0}] and capture the output, exception {1} with message {2} ", command, e, e.Message);
                Exceptions.Caught(e, Level.Error, output, Logger.GetLogger(typeof(ShellTask)));
            }

            return output;
        }

        [NamedParameter("Shell Command", "cmd", "")]
        public class Command : Name<string>
        {
        }
    }
}
