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
using System.IO;
using System.Reflection;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Util
{
    public class AssemblyLoader
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(AssemblyLoader));

        public IList<Assembly> Assemblies { get; set; }

        public AssemblyLoader(string[] files)
        {
            Assemblies = new List<Assembly>();
            foreach (var a in files)
            {
                try
                {
                    Assemblies.Add(Assembly.Load(a));
                }
                catch (FileNotFoundException exception)
                {
                    LOGGER.Log(Level.Warning, "Could not load assembly: {0} Exception: {1}", a, exception);
                }
            }
        }

        public Type GetType(string name)
        {           
            Type t = Type.GetType(name);
            if (t == null)
            {
                foreach (var a in Assemblies)
                {
                    t = a.GetType(name);
                    if (t != null)
                    {
                        return t;
                    }
                }

                foreach (var a in AppDomain.CurrentDomain.GetAssemblies())
                {
                    t = a.GetType(name);
                    if (t != null)
                    {
                        break;
                    }
                }
            }

            if (t == null)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ApplicationException("Not able to get Type from the name provided: " + name), LOGGER);
            }
            return t;
        }

        /// <summary>
        /// </summary>
        /// <param name="path"></param>
        /// <returns>True, if the path given is an assembly</returns>
        public static bool IsAssembly(string path)
        {
            if (string.IsNullOrWhiteSpace(path) || Path.GetExtension(path).ToLower() != ".dll")
            {
                return false;
            }

            try
            {
                var assembly = System.Reflection.AssemblyName.GetAssemblyName(path);
                return true;
            }
            catch (System.IO.FileNotFoundException)
            {
                return false;
            }
            catch (System.BadImageFormatException)
            {
                return false;
            }
            catch (System.IO.FileLoadException)
            {
                return true;
            }
        }
    }
}
