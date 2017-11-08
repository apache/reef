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

namespace Org.Apache.REEF.Client.API
{
    /// <summary>
    /// Represents the status of a Driver.
    /// </summary>
    internal enum DriverStatus
    {
        /// <summary>
        /// Represents the fact that the Driver status hasn't been received yet.
        /// </summary>
        UNKNOWN,

        /// <summary>
        /// Driver is initializing.
        /// </summary>
        INIT,

        /// <summary>
        /// Driver is running.
        /// </summary>
        RUNNING,

        /// <summary>
        /// Driver has cleanly exited.
        /// </summary>
        DONE,

        /// <summary>
        /// Driver is suspended.
        /// </summary>
        SUSPEND,

        /// <summary>
        /// Driver was killed.
        /// </summary>
        KILLED,

        /// <summary>
        /// Driver Failed.
        /// </summary>
        FAILED,

        /// <summary>
        /// Driver was RUNNING, but is no longer reachable for unknown reasons.
        /// </summary>
        UNKNOWN_EXITED
    }

    /// <summary>
    /// Extension methods for DriverStatus
    /// </summary>
    internal static class DriverStatusMethods
    {
        /// <summary>
        /// Indicates that the Driver is active: INIT or RUNNING.
        /// </summary>
        /// <param name="status"></param>
        /// <returns>Whether the Driver is active.</returns>
        public static bool IsActive(this DriverStatus status)
        {
            return status == DriverStatus.RUNNING || status == DriverStatus.INIT;
        }

        /// <summary>
        /// Indicates that the Driver is not active (INIT or RUNNING).
        /// </summary>
        /// <param name="status"></param>
        /// <returns>Whether the driver is not active.</returns>
        public static bool IsNotActive(this DriverStatus status)
        {
            return !IsActive(status);
        }

        /// <summary>
        /// Parses a string representation of a DriverStatus.
        /// </summary>
        /// <param name="statusString">The string to parse.</param>
        /// <returns>The DriverStatus represented in the string.</returns>
        public static DriverStatus Parse(string statusString)
        {
            return (DriverStatus)System.Enum.Parse(typeof(DriverStatus), statusString);
        }
    }
}
