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
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Protobuf;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Bridge
{
    public class ClrHandlerHelper
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ClrHandlerHelper));

        public static string[] ReefAssemblies
        {
            get
            {
                return new[] { "Microsoft.Hadoop.Avro.dll", "Org.Apache.REEF.Driver.dll", "Org.Apache.REEF.Common.dll", "Org.Apache.REEF.Utilities.dll", "Org.Apache.REEF.Network.dll", "Org.Apache.REEF.Tang.dll", "Org.Apache.REEF.Wake.dll", "Newtonsoft.Json.dll", "protobuf-net.dll" };
            }
        }

        internal static int MemoryGranularity { get; set; }

        public static ulong CreateHandler(object handler)
        {
            GCHandle gc = GCHandle.Alloc(handler);
            IntPtr intPtr = GCHandle.ToIntPtr(gc);
            ulong ul = (ulong)intPtr.ToInt64();
            return ul;
        }

        public static void FreeHandle(ulong handle)
        {
            GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
            gc.Free();
        }

        public static void SetMemoryGranuality(int granularity)
        {
            if (granularity <= 0)
            {
                var e = new ArgumentException("granularity must be a positive value, provided: " + granularity);
                Exceptions.Throw(e, LOGGER);
            }
            MemoryGranularity = granularity;
        }

        public static ulong CreateNullHandler()
        {
            return Constants.NullHandler;
        }

        public static ISet<string> GetCommandLineArguments()
        {
            using (LOGGER.LogFunction("ClrHandlerHelper::GetCommandLineArguments"))
            {
                CommandLineArguments arguments;
                try
                {                       
                    arguments = BridgeConfigurationProvider.GetBridgeInjector(null).GetInstance<CommandLineArguments>();
                }
                catch (InjectionException e)
                {
                    string error = "Cannot inject command line arguments from driver bridge configuration. ";
                    Exceptions.CaughtAndThrow(e, Level.Error, error, LOGGER);
                    throw e;
                }
                return arguments.Arguments;
            }
        }

        public static void SupplyAdditionalClassPath(params string[] classPaths)
        {
            string path = Path.Combine(Directory.GetCurrentDirectory(), Constants.GlobalUserSuppliedJavaLibraries);
            File.Delete(path);
            File.WriteAllText(path, string.Join(",", classPaths));
        }

        public static void GenerateClassHierarchy(ISet<string> clrDlls)
        {
            using (LOGGER.LogFunction("ClrHandlerHelper::GenerateClassHierarchy"))
            {
                IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(clrDlls.ToArray());
                ProtocolBufferClassHierarchy.Serialize(Constants.ClassHierarachyBin, ns);

                LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Class hierarchy written to [{0}].", Path.Combine(Directory.GetCurrentDirectory(), Constants.ClassHierarachyBin)));
            }
        }

        public static string GetAssembliesListForReefDriverApp()
        {
            using (LOGGER.LogFunction("ClrHandlerHelper::GetAssembliesListForReefDriverApp"))
            {
                string executionDirectory = Directory.GetCurrentDirectory();
                IList<string> assemblies =
                    Directory.GetFiles(Path.Combine(executionDirectory, Constants.DriverAppDirectory), "*.dll")
                             .Select(f => string.Format(CultureInfo.InvariantCulture, "\"{0}\"", Constants.DriverAppDirectory + @"\" + Path.GetFileName(f))).ToList();

                foreach (string reefAssembly in ReefAssemblies)
                {
                    if (!File.Exists(reefAssembly))
                    {
                        var e = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Assembly [{0}] needed for REEF driver not found in {1}", reefAssembly, executionDirectory));
                        Exceptions.Throw(e, LOGGER);
                    }
                    File.Copy(reefAssembly, Path.Combine(executionDirectory, Constants.DriverAppDirectory, reefAssembly), overwrite: true);
                    assemblies.Add(string.Format(CultureInfo.InvariantCulture, "\"{0}\"", Constants.DriverAppDirectory + @"\" + reefAssembly));
                }
                return string.Join(" ", assemblies);
            }
        }

        public static void CopyDllsToAppDirectory(HashSet<string> dlls)
        {
            using (LOGGER.LogFunction("ClrHandlerHelper::CopyDllsToAppDirectory"))
            {
                string executionDirectory = Directory.GetCurrentDirectory();
                Directory.CreateDirectory(Path.Combine(executionDirectory, Constants.DriverAppDirectory));
                foreach (string dll in dlls)
                {
                    string dllFile = dll;
                    if (!dll.EndsWith(".dll", StringComparison.OrdinalIgnoreCase))
                    {
                        dllFile += ".dll";
                    }
                    if (!File.Exists(dllFile))
                    {
                        var e = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Assembly [{0}] for REEF application not found in {1}", dllFile, executionDirectory));
                        Exceptions.Throw(e, LOGGER);
                    }
                    File.Copy(dllFile, Path.Combine(executionDirectory, Constants.DriverAppDirectory, dllFile), overwrite: true);
                }
            }
        }
    }
}