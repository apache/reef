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
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Driver
{
    [ClientSide]
    public sealed class DriverConfigurationSettings
    {
        // default to "ReefDevClrBridge"
        private string _driverIdentifier = "ReefDevClrBridge";

        // default to _defaultSubmissionDirectory if not provided
        private string _submissionDirectory = "reefTmp/job_" + DateTime.Now.Millisecond;

        // default to 512MB if no value is provided
        private int _driverMemory = 512;

        // folder path that contains clr dlls used by reef
        private string _clrFolder = ".";

        // folder that contains jar File provided Byte REEF
        private string _jarFileFolder = ".";

        // default to true if no value is specified
        private bool _includeHttpServer = true;

        // default to true if no value is specified
        private bool _includeNameServer = true;

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
                    throw new ArgumentException("driver memory cannot be negative value.");
                }
                _driverMemory = value;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether including name server in the config file.
        /// </summary>
        /// <value>
        ///   <c>true</c> if [including name server]; otherwise, <c>false</c>.
        /// </value>
        public bool IncludingNameServer
        {
            get { return _includeNameServer; }
            set { _includeNameServer = value; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether including HTTP server in the config file.
        /// </summary>
        /// <value>
        ///   <c>true</c> if [including HTTP server]; otherwise, <c>false</c>.
        /// </value>
        public bool IncludingHttpServer
        {
            get { return _includeHttpServer; }
            set { _includeHttpServer = value; }
        } 

        /// <summary>
        /// Driver Identifier, default to "ReefDevClrBridge" 
        /// </summary>
        public string DriverIdentifier
        {
            get { return _driverIdentifier; }
            set { _driverIdentifier = value; }
        }

        /// <summary>
        /// Driver job submission directory in (H)DFS where jar file shall be uploaded, default to a tmp directory with GUID name
        /// If set by CLR user, the user must guarantee the uniqueness of the directory across multiple jobs
        /// </summary>
        public string SubmissionDirectory
        {
            get { return _submissionDirectory; }
            set { _submissionDirectory = value; }
        }

        /// <summary>
        /// Gets or sets the CLR folder.
        /// </summary>
        /// <value>
        /// The CLR folder.
        /// </value>
        public string ClrFolder
        {
            get { return this._clrFolder; }
            set { _clrFolder = value; }
        }

        /// <summary>
        /// Gets or sets the jar file folder.
        /// </summary>
        /// <value>
        /// The jar file folder.
        /// </value>
        public string JarFileFolder
        {
            get { return this._jarFileFolder; }
            set { _jarFileFolder = value; }
        }
    }
}