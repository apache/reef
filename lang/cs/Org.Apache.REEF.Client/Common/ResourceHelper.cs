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
using System.Reflection;
using System.Resources;

namespace Org.Apache.REEF.Client.Common
{
    /// <summary>
    /// Helps with retrieval of embedded resources.
    /// See Org.Apache.REEF.Client.csproj for embedding resources and use this class to retrieve them.
    /// </summary>
    internal class ResourceHelper
    {
        private const string CouldNotRetrieveResource = "Could not retrieve resource '{0}'";
        private readonly ResourceSet _resourceSet;

        /// <summary>
        /// Given an assembly, returns a ResourceSet for it.
        /// </summary>
        /// <param name="assembly"></param>
        /// <returns>ResourceSet</returns>
        internal ResourceHelper(Assembly assembly)
        {
            var names = assembly.GetManifestResourceNames();
            if (null == names[0])
            {
                throw new ApplicationException("Could not retrieve Assembly Manifest Resource names");
            }
            var manifestResources = assembly.GetManifestResourceStream(names[0]);
            if (null == manifestResources)
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
            if (null == resource)
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
        internal string GetString(string resourceName)
        {
            return GetResource<string>(resourceName);
        }

        /// <summary>
        /// Given resource name, returns corresponding resource
        /// </summary>
        /// <param name="resourceName"></param>
        /// <returns>T</returns>
        internal byte[] GetBytes(string resourceName)
        {
            return GetResource<byte[]>(resourceName);
        }
    }
}
