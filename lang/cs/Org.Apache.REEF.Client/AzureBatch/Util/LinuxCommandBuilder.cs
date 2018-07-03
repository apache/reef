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
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.AzureBatch.Util
{
    internal sealed class LinuxCommandBuilder : AbstractCommandBuilder
    {
        private static readonly string CommandPrefix =
            "unzip " + AzureBatchFileNames.GetTaskJarFileName() + " -d 'reef/'" + ";";
        private const string ClassPathSeparator = ":";
        private const string OsCommandFormat = "/bin/sh c \"{0}\"";

        [Inject]
        private LinuxCommandBuilder(
            REEFFileNames fileNames,
            AzureBatchFileNames azureBatchFileNames) : base(fileNames, azureBatchFileNames,
                CommandPrefix, OsCommandFormat)
        {
        }

        protected override string GetDriverClasspath()
        {
            throw new NotImplementedException();
        }

        public override string GetIpAddressFilePath()
        {
            return "$AZ_BATCH_JOB_PREP_WORKING_DIR/hostip.txt";
        }

        public override string CaptureIpAddressCommandLine()
        {
            string filePath = GetIpAddressFilePath();
            return $"/bin/bash -c \"rm -f {filePath}; echo `hostname -i` > {filePath}\"";
        }
    }
}