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
#if DOTNET_BUILD
        private const string NotSupportedValue = "<UNSUPPORTED>";
        private const string NotSupportedMessage = "Machine Status is not supported on this OS";
#else
        private static readonly PerformanceCounter CpuCounter;

        private static readonly PerformanceCounter RamCounter;

        private static readonly PerformanceCounter ProcessCpuCounter;

        private static readonly Process Process;

        private static bool _checkStatus;

        static MachineStatus()
        {
            _checkStatus = true;
            Process = Process.GetCurrentProcess();
            string processName = Process.ProcessName;

            CpuCounter = CpuCounter ?? new PerformanceCounter()
            {
                CategoryName = "Processor",
                CounterName = "% Processor Time",
                InstanceName = "_Total",
            };

            RamCounter = RamCounter ?? new PerformanceCounter()
            {
                CategoryName = "Memory",
                CounterName = "Available MBytes"
            };

            ProcessCpuCounter = ProcessCpuCounter ?? new PerformanceCounter()
            {
                CategoryName = "Process",
                CounterName = "% Processor Time",
                InstanceName = processName
            };
        }
#endif

        public static string CurrentNodeCpuUsage
        {
            get
            {
#if DOTNET_BUILD
                return NotSupportedValue + "%";
#else
                return CpuCounter.NextValue() + "%";
#endif
            }
        }

        public static string AvailableMemory
        {
            get
            {
#if DOTNET_BUILD
                return NotSupportedValue + "MB";
#else
                return RamCounter.NextValue() + "MB"; 
#endif
            }
        }

        public static string CurrentProcessMemoryUsage
        {
            get
            {
#if DOTNET_BUILD
                return NotSupportedValue + "MB";
#else
                return ((float)Process.WorkingSet64 / 1000000.0).ToString(CultureInfo.InvariantCulture) + "MB";
#endif
            }
        }

        public static string PeakProcessMemoryUsage
        {
            get
            {
#if DOTNET_BUILD
                return NotSupportedValue + "MB";          
#else
                return ((float)Process.PeakWorkingSet64 / 1000000.0).ToString(CultureInfo.InvariantCulture) + "MB";
#endif
            }
        }

        // this may not be accurate if there are multiple evaluator processes running on a single machine
        public static string CurrentProcessCpuUsage
        {
            get
            {
#if DOTNET_BUILD
                return NotSupportedValue + "MB";
#else
                return ((float)ProcessCpuCounter.RawValue / 1000000.0) + "%";
#endif
            }
        }

        public override string ToString()
        {
#if DOTNET_BUILD
            return NotSupportedMessage;
#else
            string info = "No machine status information retrieved. Could be due to lack of admin right to get the info.";
            if (_checkStatus)
            {
                try
                {
                    Process.Refresh();
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
#endif
        }
    }
}
