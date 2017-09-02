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

namespace Org.Apache.REEF.Client.YARN
{
    /// <summary>
    /// Helper class for Security token.
    /// </summary>
    internal class SecurityTokenWriter
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(SecurityTokenWriter));

        private const string SecurityTokenIdFile = "SecurityTokenId";
        private const string SecurityTokenPwdFile = "SecurityTokenPwd";

        private readonly IList<byte[]> _keys = new List<byte[]>();
        private readonly IList<string> _pwds = new List<string>();

        internal const string DefaultTokenKind = "NULL";
        internal const string DefaultService = "NULL";

        /// <summary>
        /// Injectable constructor that accepts a set of serialized tokens.
        /// Each serialized token string in the set is a serialized SecurityTokenInfo with JsonConvert 
        /// </summary>
        /// <param name="serializedTokenStrings"></param>
        [Inject]
        private SecurityTokenWriter([Parameter(typeof(SecurityTokenStrings))] ISet<string> serializedTokenStrings)
        {
            ParseTokens(serializedTokenStrings);
        }

        internal string TokenKinds { get; private set; }

        internal string TokenServices { get; private set; }

        internal void WriterTokenInfo()
        {
            if (_keys.Count > 0)
            {
                WriteTokens();
                WritePasswords();
            }
        }

        private void ParseTokens(IEnumerable<string> serializedTokenStrings)
        {
            //// If the security token is not set through SecurityTokenStrings, set default to TokenKinds
            //// so that YarnREEFParamSerializer knows it should be got from SecurityTokenKindParameter
            //// Same as TokenServices.
            if (!serializedTokenStrings.Any())
            {
                TokenKinds = DefaultTokenKind;
                TokenServices = DefaultService;
            }
            else
            {
                var tokenKinds = new StringBuilder();
                var tokenServices = new StringBuilder();

                foreach (var s in serializedTokenStrings)
                {
                    var token = JsonConvert.DeserializeObject<SecurityTokenInfo>(s);
                    _keys.Add(token.SerializedKeyInfoBytes);
                    _pwds.Add(token.TokenPassword);

                    if (tokenKinds.ToString().Length >= 3)
                    {
                        tokenKinds.Append(":");
                    }
                    tokenKinds.Append(token.TokenKind);
                    tokenKinds.Append(",");
                    tokenKinds.Append(token.SerializedKeyInfoBytes.Length);

                    if (tokenServices.ToString().Length > 0)
                    {
                        tokenServices.Append(":");
                    }
                    tokenServices.Append(token.TokenService);
                }

                TokenKinds = tokenKinds.ToString();
                TokenServices = tokenServices.ToString();
            }
        }

        private void WriteTokens()
        {
            using (var writer = new BinaryWriter(File.Open(SecurityTokenIdFile, FileMode.Create)))
            {
                foreach (var t in _keys)
                {
                    Logger.Log(Level.Info, "Original securityKey length: {0}", t.Length);
                    writer.Write(t);
                }
            }
        }

        private void WritePasswords()
        {
            using (var writer = new StreamWriter(File.Open(SecurityTokenPwdFile, FileMode.Create)))
            {
                foreach (var p in _pwds)
                {
                    writer.WriteLine(p);
                }
            }
        }
    }
}
