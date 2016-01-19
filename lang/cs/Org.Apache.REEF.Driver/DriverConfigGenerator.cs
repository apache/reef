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

using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Formats.AvroConfigurationDataContract;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Protobuf;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver
{
    [ClientSide]
    public static class DriverConfigGenerator
    {
        public const string DriverConfigFile = "driver.config";
        public const string JobDriverConfigFile = "jobDriver.config";
        public const string DriverChFile = "driverClassHierarchy.bin";
        public const string HttpServerConfigFile = "httpServer.config";
        public const string NameServerConfigFile = "nameServer.config";
        public const string UserSuppliedGlobalLibraries = "userSuppliedGlobalLibraries.txt";

        /// <summary>
        /// The bridge JAR name.
        /// </summary>
        public const string JavaBridgeJarFileName = "reef-bridge-java-0.14.0-SNAPSHOT-shaded.jar";

        private static readonly Logger Log = Logger.GetLogger(typeof(DriverConfigGenerator));

        public static void DriverConfigurationBuilder(DriverConfigurationSettings driverConfigurationSettings)
        {
            ExtractConfigFromJar(driverConfigurationSettings.JarFileFolder);

            if (!File.Exists(DriverChFile))
            {
                Log.Log(Level.Warning, string.Format(CultureInfo.CurrentCulture, "There is no file {0} extracted from the jar file at {1}.", DriverChFile, driverConfigurationSettings.JarFileFolder));
                return;
            }

            if (!File.Exists(HttpServerConfigFile))
            {
                Log.Log(Level.Warning, string.Format(CultureInfo.CurrentCulture, "There is no file {0} extracted from the jar file at {1}.", HttpServerConfigFile, driverConfigurationSettings.JarFileFolder));
                return;
            }

            if (!File.Exists(JobDriverConfigFile))
            {
                Log.Log(Level.Warning, string.Format(CultureInfo.CurrentCulture, "There is no file {0} extracted from the jar file at {1}.", JobDriverConfigFile, driverConfigurationSettings.JarFileFolder));
                return;
            }

            if (!File.Exists(NameServerConfigFile))
            {
                Log.Log(Level.Warning, string.Format(CultureInfo.CurrentCulture, "There is no file {0} extracted from the jar file at {1}.", NameServerConfigFile, driverConfigurationSettings.JarFileFolder));
                return;
            }

            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();

            IClassHierarchy driverClassHierarchy = ProtocolBufferClassHierarchy.DeSerialize(DriverChFile);

            AvroConfiguration jobDriverAvroconfiguration = serializer.AvroDeserializeFromFile(JobDriverConfigFile);
            IConfiguration jobDriverConfiguration = serializer.FromAvro(jobDriverAvroconfiguration, driverClassHierarchy);

            AvroConfiguration httpAvroconfiguration = serializer.AvroDeserializeFromFile(HttpServerConfigFile);
            IConfiguration httpConfiguration = serializer.FromAvro(httpAvroconfiguration, driverClassHierarchy);

            AvroConfiguration nameAvroconfiguration = serializer.AvroDeserializeFromFile(NameServerConfigFile);
            IConfiguration nameConfiguration = serializer.FromAvro(nameAvroconfiguration, driverClassHierarchy);

            IConfiguration merged;

            if (driverConfigurationSettings.IncludingHttpServer && driverConfigurationSettings.IncludingNameServer)
            {
                merged = Configurations.MergeDeserializedConfs(jobDriverConfiguration, httpConfiguration, nameConfiguration);
            } 
            else if (driverConfigurationSettings.IncludingHttpServer)
            {
                merged = Configurations.MergeDeserializedConfs(jobDriverConfiguration, httpConfiguration);                
            }
            else if (driverConfigurationSettings.IncludingNameServer)
            {
                merged = Configurations.MergeDeserializedConfs(jobDriverConfiguration, nameConfiguration);
            }
            else
            {
                merged = jobDriverConfiguration;
            }

            var b = merged.newBuilder();

            b.BindSetEntry("org.apache.reef.driver.parameters.DriverIdentifier", driverConfigurationSettings.DriverIdentifier);
            b.Bind("org.apache.reef.driver.parameters.DriverMemory", driverConfigurationSettings.DriverMemory.ToString(CultureInfo.CurrentCulture));
            b.Bind("org.apache.reef.driver.parameters.DriverJobSubmissionDirectory", driverConfigurationSettings.SubmissionDirectory);

            // add for all the globallibaries
            if (File.Exists(UserSuppliedGlobalLibraries))
            {
                var globalLibString = File.ReadAllText(UserSuppliedGlobalLibraries);
                if (!string.IsNullOrEmpty(globalLibString))
                {
                    foreach (string fname in globalLibString.Split(','))
                    {
                        b.BindSetEntry("org.apache.reef.driver.parameters.JobGlobalLibraries", fname);
                    }
                }
            }

            foreach (string f in Directory.GetFiles(driverConfigurationSettings.ClrFolder))
            {
                b.BindSetEntry("org.apache.reef.driver.parameters.JobGlobalFiles", f);
            }

            IConfiguration c = b.Build();

            serializer.ToFile(c, DriverConfigFile);

            Log.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "driver.config is written to: {0} {1}.", Directory.GetCurrentDirectory(), DriverConfigFile));

            // additional file for easy to read
            using (StreamWriter outfile = new StreamWriter(DriverConfigFile + ".txt"))
            {
                outfile.Write(serializer.ToString(c));
            }
        }

        private static void ExtractConfigFromJar(string jarfileFolder)
        {
            string jarfile = jarfileFolder + JavaBridgeJarFileName;
            List<string> files = new List<string>();
            files.Add(DriverConfigGenerator.HttpServerConfigFile);
            files.Add(DriverConfigGenerator.JobDriverConfigFile);
            files.Add(DriverConfigGenerator.NameServerConfigFile);
            files.Add(DriverConfigGenerator.DriverChFile);
            ClrClientHelper.ExtractConfigfileFromJar(jarfile, files, ".");
        }
    }
}
