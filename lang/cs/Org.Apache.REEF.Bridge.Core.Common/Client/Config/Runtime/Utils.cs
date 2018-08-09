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

using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using System;
using System.IO;

namespace Org.Apache.REEF.Bridge.Core.Common.Client.Config.Runtime
{
    internal static class Utils
    {
        public static IConfiguration FromTextFile(string file)
        {
            return new AvroConfigurationSerializer().FromFile(file);
        }

        public static IConfiguration FromEnvironment(string environmentVariable)
        {
            var configurationPath = Environment.GetEnvironmentVariable(environmentVariable);
            if (configurationPath == null)
            {
                throw new ArgumentException($"Environment Variable {environmentVariable} not set");
            }

            if (!File.Exists(configurationPath))
            {
                throw new ArgumentException($"File located by Environment Variable {environmentVariable} cannot be read.");
            }
            return FromTextFile(configurationPath);
        }
    }
}