/*
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
using System.IO;
using System.Linq;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver
{
    /// <summary>
    /// Utility class that generates the class hierarchy for the assemblies in the `global` folder.
    /// </summary>
    internal sealed class ClassHierarchyGeneratingDriverStartObserver : IObserver<IDriverStarted>, IObserver<IDriverRestarted>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ClassHierarchyGeneratingDriverStartObserver));

        private readonly REEFFileNames _fileNames;
        private ISet<string> _assemblies;

        [Inject]
        private ClassHierarchyGeneratingDriverStartObserver(REEFFileNames fileNames, [Parameter(typeof(DriverBridgeConfigurationOptions.SetOfAssemblies))] ISet<string> assemlies)
        {
            _assemblies = assemlies;
            _fileNames = fileNames;
        }

        /// <summary>
        /// Generates the class hierarchy file
        /// </summary>
        /// <param name="value"></param>
        public void OnNext(IDriverStarted value)
        {
            GenerateClassHierarchyBin();
        }

        /// <summary>
        /// Generates the class hierarchy file
        /// </summary>
        /// <param name="value"></param>
        public void OnNext(IDriverRestarted value)
        {
            GenerateClassHierarchyBin();
        }

        /// <summary>
        /// Silently ignored, assuming that a user-bound Observer will catch it.
        /// </summary>
        /// <param name="error"></param>
        public void OnError(Exception error)
        {
            // Silently ignored, assuming that a user-bound Observer will catch it.
        }

        /// <summary>
        /// Silently ignored, assuming that a user-bound Observer will catch it.
        /// </summary>
        public void OnCompleted()
        {
            // Silently ignored, assuming that a user-bound Observer will catch it.
        }

        /// <summary>
        /// Generates the clr class hierarchy.
        /// </summary>
        private void GenerateClassHierarchyBin()
        {
            if (_assemblies != null && _assemblies.Count > 0)
            {
                //adding system level assemblies
                _assemblies.Add(typeof(IDriver).Assembly.GetName().Name);
                _assemblies.Add(typeof(ITask).Assembly.GetName().Name);

                ClrHandlerHelper.GenerateClassHierarchy(_assemblies);
            }
            else
            {
                ClrHandlerHelper.GenerateClassHierarchy(GetAssembliesInGlobalFolder());
            }
        }

        /// <summary>
        /// </summary>
        /// <returns>The paths of all assemblies in the reef/global folder.</returns>
        private ISet<string> GetAssembliesInGlobalFolder()
        {
            return new HashSet<string>(Directory.GetFiles(_fileNames.GetGlobalFolderPath())
                .Where(e => !(string.IsNullOrWhiteSpace(e)))
                .Select(Path.GetFullPath)
                .Where(File.Exists)
                .Where(IsAssembly)
                .Select(Path.GetFileNameWithoutExtension));
        }

        /// <summary>
        /// </summary>
        /// <param name="path"></param>
        /// <returns>True, if the path given is an assembly</returns>
        private static Boolean IsAssembly(string path)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                return false;
            }
            var extension = Path.GetExtension(path).ToLower();
            return extension.EndsWith("dll") || extension.EndsWith("exe");
        }
    }
}