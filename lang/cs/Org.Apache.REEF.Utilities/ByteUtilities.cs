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
using System.Text;

namespace Org.Apache.REEF.Utilities
{
    // TODO[REEF-842] Act on the obsoletes
    public class ByteUtilities
    {
        public static byte[] StringToByteArrays(string s)
        {
            return Encoding.UTF8.GetBytes(s);
        }

        [Obsolete("Deprecated in 0.14, please use ByteArraysToString instead.")]
        public static string ByteArrarysToString(byte[] b)
        {
            return ByteArraysToString(b);
        }

        public static string ByteArraysToString(byte[] b)
        {
            return Encoding.UTF8.GetString(b);
        }

        public static byte[] CopyBytesFrom(byte[] from)
        {
            int length = Buffer.ByteLength(from);
            byte[] to = new byte[length];
            Buffer.BlockCopy(from, 0, to, 0, length);
            return to;
        }
    }
}
