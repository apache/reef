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
using System.IO;

using Org.Apache.REEF.Common.Files;
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

        private bool _pidIsWritten = false;

        private object lockObject = new object();

        private REEFFileNames _reefFileNames;

        [Inject]
        private PIDStoreHandler(REEFFileNames reefFileNames)
        {
            _reefFileNames = reefFileNames;
        }

        /// <summary>
        /// Writes PID to the runtime folder
        /// </summary>
        public void WritePID()
        {
            lock (lockObject)
            {
                if (!_pidIsWritten)
                {
                    string currentDirectory = Directory.GetCurrentDirectory();
                    string path = Path.Combine(currentDirectory, _reefFileNames.GetPidFileName());

                    try
                    {
                        var pid = Process.GetCurrentProcess().Id;
                        using (StreamWriter sw = File.CreateText(path))
                        {
                            sw.WriteLine(pid);
                            sw.Flush();
                        }

                        Logger.Log(Level.Verbose, "Writing PID {0} to file {1}", pid, path);
                        _pidIsWritten = true;
                    }
                    catch (Exception e)
                    {
                        Logger.Log(Level.Error, "Failed writing PID with exception {0}", e);                        
                    }
                }
            }
        }
    }
}
