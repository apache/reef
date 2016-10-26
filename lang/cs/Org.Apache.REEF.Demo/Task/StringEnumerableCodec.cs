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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Demo.Task
{
    internal sealed class StringEnumerableCodec : ICodec<IEnumerable<string>>
    {
        private const int LengthOfEncodedInteger = 4;

        [Inject]
        private StringEnumerableCodec()
        {
        }

        public byte[] Encode(IEnumerable<string> strings)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                foreach (string str in strings)
                {
                    byte[] bytes = ByteUtilities.StringToByteArrays(str);
                    byte[] lengthInBytes = BitConverter.GetBytes(bytes.Length);
                    stream.Write(lengthInBytes, 0, lengthInBytes.Length); // lengthInBytes.Length == LengthOfEncodedInteger
                    stream.Write(bytes, 0, bytes.Length);
                }

                return stream.ToArray();
            }
        }

        public IEnumerable<string> Decode(byte[] data)
        {
            using (MemoryStream stream = new MemoryStream(data))
            {
                IList<string> retList = new List<string>();
                while (stream.Position < stream.Length)
                {
                    byte[] lengthInBytes = new byte[LengthOfEncodedInteger];
                    stream.Read(lengthInBytes, 0, lengthInBytes.Length);

                    int length = BitConverter.ToInt32(lengthInBytes, 0);
                    byte[] bytes = new byte[length];
                    stream.Read(bytes, 0, bytes.Length);
                    string str = ByteUtilities.ByteArraysToString(bytes);

                    retList.Add(str);
                }

                return retList;
            }
        }
    }
}
