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

using System.Diagnostics;
using System.IO;

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Runtime.Evaluator
{
    /// <summary>
    /// A PID store handler for c# evaluator
    /// </summary>
    public sealed class PIDStoreHandler
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(PIDStoreHandler));

        /// <summary>
        /// The file name of the PID file created in the current working directory of the process.
        /// This is similar to the file name in teh PIDStoreHandler.java
        /// </summary>
        public const string PidFileName = "PID.txt";

        private bool _pidIsWritten = false;

        [Inject]
        private PIDStoreHandler()
        {
        }

        /// <summary>
        /// 
        /// </summary>
        public void WritePID()
        {
            lock (this)
            {
                if (!_pidIsWritten)
                {
                    string currentDirectory = Directory.GetCurrentDirectory();
                    string path = currentDirectory + @"\" + PidFileName;
                    var pid = Process.GetCurrentProcess().Id;
                    using (StreamWriter sw = File.CreateText(path))
                    {
                        sw.WriteLine(pid);
                        sw.Flush();
                    }

                    Logger.Log(Level.Error, "Writing PID {0} to file {1}", pid, path);
                    _pidIsWritten = true;
                }
            }
        }
    }
}
