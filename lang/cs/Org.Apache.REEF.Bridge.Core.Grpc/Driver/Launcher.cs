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

using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Time;
using System;
using System.IO;

namespace Org.Apache.REEF.Bridge.Core.Grpc.Driver
{
    /// <summary>
    /// Launcher can be referenced by outside aseemblies that want to launch
    /// the client driver by calling the Main method.
    /// </summary>
    public sealed class Launcher
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(Launcher));

        private readonly REEFFileNames _reefFileNames;

        private readonly IConfigurationSerializer _configurationSerializer;

        [Inject]
        private Launcher(
            REEFFileNames reefFileNames,
            IConfigurationSerializer configurationSerializer)
        {
            _reefFileNames = reefFileNames;
            _configurationSerializer = configurationSerializer;
        }

        private IConfiguration GetDriverClientConfiguration(string driverServicePort)
        {
            var configFile = Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(),
                _reefFileNames.GetClrDriverConfigurationPath()));
            if (File.Exists(configFile))
            {
                // If we reached this, we weren't able to find the configuration file.
                var message = $"Unable to find brigde configuration. Paths checked: [\'{configFile}\']";

                Log.Log(Level.Error, message);
                throw new FileNotFoundException(message);
            }
            else
            {
                return Configurations.Merge(_configurationSerializer.FromFile(configFile),
                    TangFactory.GetTang().NewConfigurationBuilder()
                        .BindNamedParameter(typeof(DriverServicePort), driverServicePort)
                        .Build());
            }
        }

        public static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                const string message = "Grpc launcher takes a single argument service port value";
                Log.Log(Level.Error, message);
                throw new ArgumentException(message);
            }
            else
            {
                Log.Log(Level.Info, "Launching driver client with driver service port " + args[0]);
            }
            var launcher = TangFactory.GetTang().NewInjector().GetInstance<Launcher>();
            var driverClientConfig = launcher.GetDriverClientConfiguration(args[0]);
            var clock = TangFactory.GetTang().NewInjector(driverClientConfig).GetInstance<IClock>();
            clock.Run();

            Log.Log(Level.Info, "Driver client clock exit.");
        }
    }
}