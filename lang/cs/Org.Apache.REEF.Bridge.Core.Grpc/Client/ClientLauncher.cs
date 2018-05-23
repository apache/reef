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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Org.Apache.REEF.Bridge.Core.Common.Client;
using Org.Apache.REEF.Bridge.Core.Common.Client.Config.Runtime.Proto;
using Org.Apache.REEF.Bridge.Core.Common.Driver;
using Org.Apache.REEF.Bridge.Core.Grpc.Driver;
using Org.Apache.REEF.Bridge.Core.Proto;
using Org.Apache.REEF.Common.Client;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Bridge.Core.Grpc.Client
{
    internal sealed class ClientLauncher : IClientLauncher
    {
        private const string SoFileNameExtension = ".so";
        private const string DylibFileNameExtension = ".dylib";
        private const string DllFileNameExtension = ".dll";
        private const string ExeFileNameExtension = ".exe";
        private const string JarFileNameExtension = ".jar";
        private const string ConfigFileNameExtension = ".config";

        private const string JavaClientLauncherClass = "org.apache.reef.bridge.client.grpc.ClientLauncher";

        private const string DriverExe = "Org.Apache.REEF.Bridge.Core.Driver.exe";
        private const string DriverDll = "Org.Apache.REEF.Bridge.Core.Driver.dll";

        private static readonly string jsonSDK = @"
{
  ""runtimeOptions"": {
        ""tfm"": ""netcoreapp2.0"",
        ""framework"": {
            ""name"": ""Microsoft.NETCore.App"",
            ""version"": ""2.0.0""
        }
    }
}
";

        private static readonly Logger Log = Logger.GetLogger(typeof(ClientLauncher));

        private readonly ClientService _clientService;

        private readonly DriverClientConfiguration _driverClientConfiguration;

        private readonly REEFFileNames _reefFileNames;

        private readonly IConfigurationSerializer _configurationSerializer;

        private readonly JavaClientLauncher _javaClientLauncher;

        private readonly Server _grpcServer;

        private readonly int _grpcServerPort;

        [Inject]
        private ClientLauncher(
            ClientService clientService,
            JavaClientLauncher javaClientLauncher,
            REEFFileNames reefFileNames,
            IConfigurationSerializer configurationSerializer,
            DriverClientParameters driverRuntimeProto,
            IRuntimeProtoProvider runtimeProtoProvider)
        {
            _clientService = clientService;
            _javaClientLauncher = javaClientLauncher;
            _reefFileNames = reefFileNames;
            _configurationSerializer = configurationSerializer;
            _driverClientConfiguration = driverRuntimeProto.Proto;
            runtimeProtoProvider.SetParameters(_driverClientConfiguration);
            _grpcServer = new Server
            {
                Services = { BridgeClient.BindService(clientService) },
                Ports = { new ServerPort("localhost", 0, ServerCredentials.Insecure) }
            };
            _grpcServer.Start();
            Log.Log(Level.Info, "Server port any {0}", _grpcServer.Ports.Any());
            foreach (var serverPort in _grpcServer.Ports)
            {
                Log.Log(Level.Info, "Server port {0}", serverPort.BoundPort);
                _grpcServerPort = serverPort.BoundPort;
            }
        }

        public void Dispose()
        {
            _grpcServer.ShutdownAsync();
        }

        public async Task<LauncherStatus> SubmitAsync(
            IConfiguration driverAppConfiguration, 
            CancellationToken cancellationToken)
        {
            var driverClientConfiguration =
                Configurations.Merge(driverAppConfiguration,
                    DriverBridgeConfiguration.ConfigurationModule
                        .Set(DriverBridgeConfiguration.DriverServiceClient, GenericType<DriverServiceClient>.Class)
                        .Set(DriverBridgeConfiguration.DriverClientService, GenericType<DriverClientService>.Class)
                        .Build());
            var jobFolder = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString()));
            Log.Log(Level.Info, "Job folder {0}", jobFolder);
            Directory.CreateDirectory(jobFolder.FullName);
            AddDependencies(jobFolder, driverClientConfiguration);
            // Launch command
            if (File.Exists(DriverExe))
            {
                _driverClientConfiguration.DriverClientLaunchCommand = 
                    "cmd.exe /c " + Path.Combine(_reefFileNames.GetGlobalFolderPath(), DriverExe) + " " + 
                    _reefFileNames.GetClrDriverConfigurationPath();
            }
            else
            {
                _driverClientConfiguration.DriverClientLaunchCommand = 
                    "dotnet " + Path.Combine(_reefFileNames.GetGlobalFolderPath(), DriverDll) + " " + 
                    _reefFileNames.GetClrDriverConfigurationPath();
            }

            var driverClientConfigFile = Path.Combine(jobFolder.FullName, "driverclient.json");
            using (var outputFile = new StreamWriter(driverClientConfigFile))
            {
                outputFile.Write(JsonFormatter.Default.Format(_driverClientConfiguration));
            }
            // Submit a new job
            _clientService.Reset(); 
            var task = _javaClientLauncher.LaunchAsync(JavaLoggingSetting.Info,
                JavaClientLauncherClass,
                new[] { driverClientConfigFile, _grpcServerPort.ToString() },
                cancellationToken);
            lock (_clientService)
            {
                while (!_clientService.IsDone && !cancellationToken.IsCancellationRequested)
                {
                    Monitor.Wait(_clientService, TimeSpan.FromMinutes(1));
                }
            }
            await task;
            return _clientService.LauncherStatus;
        }

        private void AddDependencies(FileSystemInfo jobFolder, IConfiguration driverClientConfiguration)
        {
            // driver client configuration
            var driverClientConfigurationFile = Path.GetFullPath(Path.Combine(
                jobFolder.FullName,
                _reefFileNames.GetClrDriverConfigurationName()));
            _configurationSerializer.ToFile(driverClientConfiguration, driverClientConfigurationFile);
            _driverClientConfiguration.LocalFiles.Add(driverClientConfigurationFile);

            /*
            var jsonSDKFile = Path.Combine(jobFolder.FullName, "Org.Apache.REEF.Bridge.Core.Driver.runtimeconfig.json");
            File.WriteAllText(jsonSDKFile, jsonSDK);
            _driverClientConfiguration.GlobalFiles.Add(jsonSDKFile);
            */

            // resource files
            var a = typeof(ClientLauncher).Assembly;
            foreach (var name in a.GetManifestResourceNames())
            {
                Log.Log(Level.Info, "Extracting resource {0}", name);
                var resource = a.GetManifestResourceStream(name);
                using (var file = new FileStream(name, FileMode.Create, FileAccess.Write))
                {
                    resource.CopyTo(file);
                }
            }

            var directory = ClientUtilities.GetPathToExecutingAssembly();
            {
                if (Directory.Exists(directory))
                {
                    // For input paths that are directories, extract only files of a predetermined type
                    foreach (var assembly in Directory.GetFiles(directory).Where(IsAssemblyToCopy))
                    {
                        _driverClientConfiguration.GlobalFiles.Add(assembly);
                    }
                }
                else
                {
                    // Throw if a path input was not a file or a directory
                    throw new FileNotFoundException($"Global Assembly Directory not Found: {directory}");
                }
            }
        }

        /// <summary>
        /// Returns true, if the given file path references a DLL or EXE or JAR.
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        private static bool IsAssemblyToCopy(string filePath)
        {
            var fileName = Path.GetFileName(filePath);
            if (string.IsNullOrWhiteSpace(fileName))
            {
                return false;
            }

            var lowerCasePath = fileName.ToLower();
            return lowerCasePath.EndsWith(DllFileNameExtension) ||
                   lowerCasePath.EndsWith(ExeFileNameExtension) ||
                   lowerCasePath.EndsWith(DylibFileNameExtension) ||
                   lowerCasePath.EndsWith(SoFileNameExtension) ||
                   lowerCasePath.EndsWith(ConfigFileNameExtension) ||
                   lowerCasePath.EndsWith(JarFileNameExtension);
        }
    }
}
