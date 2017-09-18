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
using System.Text;
using Newtonsoft.Json;
using Org.Apache.REEF.Client.YARN.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;
using Microsoft.Hadoop.Avro;
using Org.Apache.REEF.Client.Avro.YARN;

namespace Org.Apache.REEF.Client.YARN
{
    /// <summary>
    /// Serialize a set of security tokens into a file.
    /// </summary>
    internal class SecurityTokenWriter
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(SecurityTokenWriter));

        private const string SecurityTokenFile = "SecurityTokens.json";

        private readonly IAvroSerializer<SecurityToken> _avroSerializer = AvroSerializer.Create<SecurityToken>();
        private readonly List<SecurityToken> _tokens;

        /// <summary>
        /// Injectable constructor that accepts a set of serialized tokens.
        /// Each serialized token string in the set is a serialized SecurityTokenInfo by JsonConvert 
        /// </summary>
        /// <param name="serializedTokenStrings"></param>
        [Inject]
        private SecurityTokenWriter([Parameter(typeof(SecurityTokenStrings))] ISet<string> serializedTokenStrings)
        {
            _tokens = serializedTokenStrings.Select(serializedToken =>
            {
                var token = JsonConvert.DeserializeObject<SecurityTokenInfo>(serializedToken);
                return new SecurityToken(token.TokenKind, token.TokenService,
                    token.SerializedKeyInfoBytes, Encoding.ASCII.GetBytes(token.TokenPassword));
            }).ToList();
        }

        /// <summary>
        /// Write SecurityTokens to SecurityTokenFile using IAvroSerializer.
        /// </summary>
        public void WriteTokensToFile()
        {
            if (_tokens != null && _tokens.Count > 0)
            {
                Logger.Log(Level.Verbose, "Write {0} tokens to file.", _tokens.Count);
                using (var stream = File.OpenWrite(SecurityTokenFile))
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
