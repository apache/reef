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
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using Org.Apache.REEF.Client.YARN.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;
using Microsoft.Hadoop.Avro;
using Org.Apache.REEF.Client.Avro.YARN;
using Org.Apache.REEF.Common.Files;

namespace Org.Apache.REEF.Client.YARN
{
    /// <summary>
    /// Serialize a set of security tokens into a file.
    /// </summary>
    internal class SecurityTokenWriter
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(SecurityTokenWriter));

        private readonly IAvroSerializer<SecurityToken> _avroSerializer = AvroSerializer.Create<SecurityToken>();
        private readonly List<SecurityToken> _tokens;

        private readonly string _securityTokensFile;

        /// <summary>
        /// Injectable constructor that accepts a set of serialized tokens.
        /// Each serialized token string in the set is a serialized SecurityToken by JsonConvert 
        /// </summary>
        /// <param name="serializedTokenStrings">Serialized token strings</param>
        /// <param name="reefFileNames">REEF file name constants</param>
        [Inject]
        private SecurityTokenWriter(REEFFileNames reefFileNames,
            [Parameter(typeof(SecurityTokenStrings))] ISet<string> serializedTokenStrings)
        {
            _securityTokensFile = reefFileNames.GetSecurityTokenFileName();
            _tokens = serializedTokenStrings.Select(serializedToken =>
            {
                var token = JsonConvert.DeserializeObject<SecurityToken>(serializedToken);
                return new SecurityToken(token.kind, token.service, token.key, token.password);
            }).ToList();
        }

        /// <summary>
        /// Write SecurityToken objects to SecurityTokenFile using IAvroSerializer.
        /// </summary>
        public void WriteTokensToFile()
        {
            Logger.Log(Level.Verbose, "Write {0} tokens to file: {1}.", _tokens.Count, _securityTokensFile);

            if (_tokens.Count > 0)
            {
                using (var stream = File.OpenWrite(_securityTokensFile))
                {
                    foreach (var token in _tokens)
                    {
                        Logger.Log(Level.Verbose, "Write token {0} to file.", token.kind);
                        _avroSerializer.Serialize(stream, token);
                    }
                }
            }
        }
    }
}
