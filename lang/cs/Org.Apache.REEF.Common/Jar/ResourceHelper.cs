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
using System.Collections.Generic;
using System.Reflection;
using System.Resources;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Jar
{
    /// <summary>
    /// Helps with retrieval of embedded resources.
    /// See Org.Apache.REEF.Client.csproj for embedding resources and use this class to retrieve them.
    /// </summary>
    [Private]
    public class ResourceHelper
    {
        public const string ClientJarFullName = "ClientJarFullName";
        public const string DriverJarFullName = "DriverJarFullName";
        public const string ClrDriverFullName = "ClrDriverFullName";
        public const string BridgeInteropDllFullName = "BridgeInteropDllFullName";

        // We embed certain binaries in client dll.
        // Following items in tuples refer to resource names in Org.Apache.REEF.Client.dll
        // The first resource item contains the name of the file 
        // such as "reef-bridge-java-<version>-shaded.jar". The second resource
        // item contains the byte contents for said file.
        // Please note that the identifiers below need to be in sync with 2 other files
        // 1. $(SolutionDir)\Org.Apache.REEF.Client\Properties\Resources.xml
        // 2. $(SolutionDir)\Org.Apache.REEF.Client\Org.Apache.REEF.Client.csproj
        public readonly static Dictionary<string, string> FileResources = new Dictionary<string, string>
        {
            { ClientJarFullName, "reef_bridge_client" },
            { DriverJarFullName, "reef_bridge_driver" },
            { ClrDriverFullName, "reef_clrdriver" },
            { BridgeInteropDllFullName, "reef_bridge_interop" },
        };

        private const string CouldNotRetrieveResource = "Could not retrieve resource '{0}'";
        private readonly ResourceSet _resourceSet;

        /// <summary>
        /// Given an assembly, returns a ResourceSet for it.
        /// </summary>
        /// <param name="assembly"></param>
        /// <returns>ResourceSet</returns>
        public ResourceHelper(Assembly assembly)
        {
            var names = assembly.GetManifestResourceNames();
            if (names == null || names.Length == 0 || names[0] == null)
            {
                throw new ApplicationException("Could not retrieve Assembly Manifest Resource names");
            }
            var manifestResources = assembly.GetManifestResourceStream(names[0]);
            if (manifestResources == null)
            {
                throw new ApplicationException("Could not retrieve Assembly Manifest Resource stream");
            }

            _resourceSet = new ResourceSet(manifestResources);
        }

        /// <summary>
        /// Given resource name, returns corresponding resource
        /// </summary>
        /// <param name="resourceName"></param>
        /// <returns>T</returns>
        internal T GetResource<T>(string resourceName)
        {
            var resource = _resourceSet.GetObject(resourceName);
            if (resource == null)
            {
                throw new ApplicationException(string.Format(CouldNotRetrieveResource, resourceName));
            }
            return (T)resource;
        }

        /// <summary>
        /// Given resource name, returns corresponding resource
        /// </summary>
        /// <param name="resourceName"></param>
        /// <returns>T</returns>
        public string GetString(string resourceName)
        {
            return GetResource<string>(resourceName);
        }

        /// <summary>
        /// Given resource name, returns corresponding resource
        /// </summary>
        /// <param name="resourceName"></param>
        /// <returns>T</returns>
        public byte[] GetBytes(string resourceName)
        {
            return GetResource<byte[]>(resourceName);
        }
    }
}
