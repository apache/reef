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
using System.Globalization;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Runtime
{
    public class MachineStatus
    {
        private static readonly PerformanceCounter _cpuCounter;

        private static readonly PerformanceCounter _ramCounter;

        private static readonly PerformanceCounter _processCpuCounter;

        private static readonly Process _process;

        private static bool _checkStatus;

        static MachineStatus()
        {
            _checkStatus = true;
            _process = Process.GetCurrentProcess();
            string processName = _process.ProcessName;

            _cpuCounter = _cpuCounter ?? new PerformanceCounter()
            {
                CategoryName = "Processor",
                CounterName = "% Processor Time",
                InstanceName = "_Total",
            };

            _ramCounter = _ramCounter ?? new PerformanceCounter()
            {
                CategoryName = "Memory",
                CounterName = "Available MBytes"
            };

            _processCpuCounter = _processCpuCounter ?? new PerformanceCounter()
            {
                CategoryName = "Process",
                CounterName = "% Processor Time",
                InstanceName = processName
            };
        }

        public static string CurrentNodeCpuUsage
        {
            get
            {
                return _cpuCounter.NextValue() + "%";
            }
        }

        public static string AvailableMemory
        {
            get
            {
                return _ramCounter.NextValue() + "MB";
            }
        }

        public static string CurrentProcessMemoryUsage
        {
            get
            {
                return ((float)_process.WorkingSet64 / 1000000.0).ToString(CultureInfo.InvariantCulture) + "MB";
            }
        }

        public static string PeakProcessMemoryUsage
        {
            get
            {
                return ((float)_process.PeakWorkingSet64 / 1000000.0).ToString(CultureInfo.InvariantCulture) + "MB";
            }
        }

        // this may not be accurate if there are multiple evaluator processes running on a single machine
        public static string CurrentProcessCpuUsage
        {
            get
            {
                return ((float)_processCpuCounter.RawValue / 1000000.0) + "%";
            }
        }

        public override string ToString()
        {
            string info = "No machine status information retrieved. Could be due to lack of admin right to get the info.";
            if (_checkStatus)
            {
                try
                {
                    _process.Refresh();
                    info = string.Format(
                    CultureInfo.InvariantCulture,
                    "current node is running at [{0}] CPU usage and with [{1}] memory available.{2}             current evaluator process is using [{3}] of CPU and [{4}] of memory, with a peak memory usage of [{5}]",
                    CurrentNodeCpuUsage,
                    AvailableMemory,
                    Environment.NewLine,
                    CurrentProcessCpuUsage,
                    CurrentProcessMemoryUsage,
                    PeakProcessMemoryUsage);
                }
                catch (Exception e)
                {
                    _checkStatus = false; // It only takes one exception to switch the checking off for good.
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Warning, "Cannot obtain machine status due to error", Logger.GetLogger(typeof(MachineStatus)));

                    // we do not want to crash the evaluator just because we cannot get the information.
                    info = "Cannot obtain machine status due to error " + e.Message;
                }
            }
            
            return info;
        }
    }
}
