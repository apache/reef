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
using System.IO;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Utilities.Diagnostics
{
    /// <summary>
    /// A static class that allows the registering/unregistering of the default UnhandledException
    /// Handler, which logs the Exception and lists all files in the current working directory.
    /// </summary>
    internal static class DefaultUnhandledExceptionHandler
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(DefaultUnhandledExceptionHandler));

        /// <summary>
        /// Registers the default unhandled Exception handler, which logs the Exception
        /// and lists all files in the current working directory.
        /// </summary>
        public static void Register()
        {
            AppDomain.CurrentDomain.UnhandledException += Handler;
        }

        /// <summary>
        /// Unregisters the default unhandled Exception handler.
        /// </summary>
        public static void Unregister()
        {
            AppDomain.CurrentDomain.UnhandledException -= Handler;
        }

        private static void Handler(object sender, UnhandledExceptionEventArgs e)
        {
            var message = string.Format(
                "Unhandled exception {0}. Current files in the working directory: {1}",
                e.ExceptionObject,
                string.Join(", ", Directory.EnumerateFiles(Directory.GetCurrentDirectory(), "*.*", SearchOption.AllDirectories)));

            Logger.Log(Level.Error, message);
        }
    }
}