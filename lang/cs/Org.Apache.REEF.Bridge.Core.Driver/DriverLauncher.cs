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
using Grpc.Core;
using Grpc.Core.Logging;
using Org.Apache.REEF.Bridge.Core.Grpc.Driver;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Time;

namespace Org.Apache.REEF.Bridge.Core.Driver
{
    /// <summary>
    /// Used to launch the C# driver client.
    /// </summary>
    public sealed class DriverLauncher
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(DriverLauncher));

        private readonly IConfigurationSerializer _configurationSerializer;

        [Inject]
        private DriverLauncher(IConfigurationSerializer configurationSerializer)
        {
            _configurationSerializer = configurationSerializer;
        }

        private IConfiguration GetDriverClientConfiguration(string configFile, string driverServicePort)
        {
            if (!File.Exists(configFile))
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
            if (args.Length != 2)
            {
                var message = $"Driver client launcher takes a two arguments. Found {args}";
                Log.Log(Level.Error, message);
                throw new ArgumentException(message);
            }
            else
            {
                Log.Log(Level.Info, "Launching driver client with driver service port {0} config file {1}", args[0], args[1]);
            }
            GrpcEnvironment.SetLogger(new ConsoleLogger());
            Log.Log(Level.Info, "Path of executing assembly{0}", ClientUtilities.GetPathToExecutingAssembly());
            var launcher = TangFactory.GetTang().NewInjector().GetInstance<DriverLauncher>();
            var driverClientConfig = launcher.GetDriverClientConfiguration(args[0], args[1]);
            var clock = TangFactory.GetTang().NewInjector(driverClientConfig).GetInstance<IClock>();
            clock.Run();

            Log.Log(Level.Info, "Driver client clock exit.");
        }
    }
}
