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
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver
{
    // TODO: merge with EvaluatorConfigurations class
    // TODO[REEF-842] Act on the obsoletes
    [Obsolete("Deprecated in 0.14. Please use DriverConfigurationSettings instead.")]
    public class DriverSubmissionSettings
    {
        // default to "ReefDevClrBridge"
        private string _driverIdentifier;

        // default to _defaultSubmissionDirectory is not provided
        private string _submissionDirectory;

        // deault to 512MB if no value is provided
        private int _driverMemory = 0;

        // default value, client wait till driver exit
        private int _clientWaitTime = -1;

        // default to submit to driver with driver config
        private bool _submit = true;

        // default to always update jar before submission
        private bool _updateJar = true;

        // default to run on local
        private bool _runOnYarn;

        // default to set to info logging
        private JavaLoggingSetting _javaLogLevel = JavaLoggingSetting.INFO;

        /// <summary>
        /// Whether to update jar file with needed dlls before submission
        /// User can choose to reduce startup time by skipping the update, if jar file already contains all necessary dll
        /// Note this settig is .NET only, it does not propagate to java side.
        /// </summary>
        public bool UpdateJarBeforeSubmission 
        {
            get { return _updateJar; }
            set { _updateJar = value; }
        }

        /// <summary>
        /// Determine the vebosity of Java driver's log.
        /// Note this parameter is used when launching java process only, it does not propagate to java side.
        /// </summary>
        public JavaLoggingSetting JavaLogLevel
        {
            get { return _javaLogLevel; }
            set { _javaLogLevel = value; }
        }

        /// <summary>
        /// Memory allocated for driver, default to 512 MB
        /// </summary>
        public int DriverMemory
        {
            get
            {
                return _driverMemory;
            }

            set
            {
                if (value < 0)
                {
                    throw new ArgumentException("driver memory cannot be negatvie value.");
                }
                _driverMemory = value;
            }
        }

        /// <summary>
        /// Driver Identifier, default to "ReefDevClrBridge" 
        /// </summary>
        public string DriverIdentifier
        {
            get
            {
                return _driverIdentifier;
            }

            set
            {
                _driverIdentifier = value;
            }
        }

        /// <summary>
        /// Whether to submit driver with config after driver configuration is construted, default to True
        /// </summary>
        public bool Submit
        {
            get
            {
                return _submit;
            }

            set
            {
                _submit = value;
            }
        }

        /// <summary>
        /// How long client would wait for Driver, default to wait till  driver is done
        /// </summary>
        public int ClientWaitTime
        {
            get
            {
                return _clientWaitTime;
            }

            set
            {
                _clientWaitTime = value;
            }
        }

        /// <summary>
        /// Driver job submission directory in (H)DFS where jar file shall be uploaded, default to a tmp directory with GUID name
        /// If set by CLR user, the user must guarantee the uniquness of the directory across multiple jobs
        /// </summary>
        public string SubmissionDirectory
        {
            get
            {
                return _submissionDirectory;
            }

            set
            {
                _submissionDirectory = value;
            }
        }

        /// <summary>
        /// Whether to Run on YARN runtime, default to false
        /// </summary>
        public bool RunOnYarn
        {
            get
            {
                return _runOnYarn;
            }

            set
            {
                _runOnYarn = value;
            }
        }

        [Obsolete("Deprecated in 0.14, please use ToCommandLineArguments instead.")]
        public string ToComamndLineArguments()
        {
            return ToCommandLineArguments();
        }

        public string ToCommandLineArguments()
        {
            return
                (RunOnYarn ? " -local false" : string.Empty) +
                (!Submit ? " -submit false" : string.Empty) +
                (DriverMemory > 0 ? " -driver_memory " + DriverMemory : string.Empty) +
                (!string.IsNullOrWhiteSpace(DriverIdentifier) ? " -drive_id " + DriverIdentifier : string.Empty) +
                (ClientWaitTime > 0 ? " -wait_time " + ClientWaitTime : string.Empty) +
                (!string.IsNullOrWhiteSpace(SubmissionDirectory) ? " -submission_directory " + SubmissionDirectory : string.Empty);
        }
    }
}
