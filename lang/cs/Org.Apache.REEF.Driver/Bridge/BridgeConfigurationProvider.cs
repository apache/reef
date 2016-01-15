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

using System.IO;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Bridge
{
    /// <summary>
    /// Helper class that provides the Bridge Configuration.
    /// </summary>
    /// <remarks>
    /// The Bridge configuration file moved in the evolution of REEF (see [REEF-228]). This class hides the actual location
    /// from client code and does the appropriate logging of its location.
    /// </remarks>
    internal sealed class BridgeConfigurationProvider
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(BridgeConfigurationProvider));
        private static readonly object LockObject = new object();

        private static IConfiguration bridgeConfiguration = null;
        private static IInjector bridgeInjector = null;

        private readonly REEFFileNames _fileNames;

        [Inject]
        internal BridgeConfigurationProvider(REEFFileNames fileNames)
        {
            _fileNames = fileNames;
        }

        /// <summary>
        /// Finds the path to the bridge configuration
        /// </summary>
        /// <remarks>
        /// It tries both the new and the legacy locations and gives preference to the new locations. Warnings will be logged
        /// when both are present as well as when the configuration is only present in the legacy location.
        /// </remarks>
        /// <exception cref="FileNotFoundException">When neither the legacy nor the new file exists.</exception>
        /// <returns>The path to the bridge configuration</returns>
        internal string GetBridgeConfigurationPath()
        {
            var newBridgeConfigurationPath = Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(),
                _fileNames.GetClrDriverConfigurationPath()));
            var legacyBridgeConfigurationPath =
                Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "reef", "global",
                    "clrBridge.config"));

            var newExists = File.Exists(newBridgeConfigurationPath);
            var oldExists = File.Exists(legacyBridgeConfigurationPath);

            if (newExists && oldExists)
            {
                var msg = "Found configurations in both the legacy location (" + legacyBridgeConfigurationPath + 
                    ") and the new location (" + newBridgeConfigurationPath +
                    "). Loading only the one found in the new location.";
                Logger.Log(Level.Warning, msg);
            }
            if (newExists)
            {
                return newBridgeConfigurationPath;
            }
            if (oldExists)
            {
                var msg = "Only found configuration in the legacy location (" + legacyBridgeConfigurationPath + 
                    ") and not the new location (" + newBridgeConfigurationPath +
                    "). Loading only the one found in the legacy location.";
                Logger.Log(Level.Warning, msg);
                return legacyBridgeConfigurationPath;
            }

            // If we reached this, we weren't able to find the configuration file.
            var message = "Unable to find brigde configuration. Paths checked: ['" +
                          newBridgeConfigurationPath + "', '" +
                          legacyBridgeConfigurationPath + "']";

            Logger.Log(Level.Error, message);
            var exception = new FileNotFoundException(message);
            Exceptions.Throw(exception, Logger);
            throw exception;
        }

        /// <summary>
        /// Loads the bridge configuration from disk.
        /// </summary>
        /// <remarks>
        /// It tries both the new and the legacy locations and gives preference to the new locations. Warnings will be logged
        /// when both are present as well as when the configuration is read from the legacy location.
        /// </remarks>
        /// <exception cref="FileNotFoundException">When neither the legacy nor the new file exists.</exception>
        /// <returns>The bridge Configuration loaded from disk</returns>
        internal IConfiguration LoadBridgeConfiguration()
        {
            var bridgeConfigurationPath = GetBridgeConfigurationPath();
            Logger.Log(Level.Info, "Loading configuration '" + bridgeConfigurationPath + "'.");
            return new AvroConfigurationSerializer().FromFile(bridgeConfigurationPath);
        }

        /// <summary>
        /// Same as LoadBridgeConfiguration for use in cases where Tang isn't available.
        /// </summary>
        /// <returns></returns>
        internal static IConfiguration GetBridgeConfiguration()
        {
            lock (LockObject)
            {
                if (bridgeConfiguration == null)
                {
                    bridgeConfiguration = new BridgeConfigurationProvider(new REEFFileNames()).LoadBridgeConfiguration();
                }

                return bridgeConfiguration;
            }
        }

        /// <summary>
        /// Instantiates an IInjector using the bridge configuration.
        /// </summary>
        /// <returns></returns>
        internal static IInjector GetBridgeInjector(IEvaluatorRequestor evaluatorRequestor)
        {
            lock (LockObject)
            {
                if (bridgeInjector == null)
                {
                    bridgeInjector = TangFactory.GetTang().NewInjector(GetBridgeConfiguration());
                    if (evaluatorRequestor != null)
                    {
                        bridgeInjector.BindVolatileInstance(GenericType<IEvaluatorRequestor>.Class, evaluatorRequestor);
                    }
                }

                return bridgeInjector;
            }
        }
    }
}