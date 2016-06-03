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
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace Org.Apache.REEF.Utilities
{
    public static class ByteUtilities
    {
        /// <summary>
        /// Converts a string to a UTF-8 encoded byte array.
        /// </summary>
        public static byte[] StringToByteArrays(string s)
        {
            return Encoding.UTF8.GetBytes(s);
        }

        /// <summary>
        /// Returns true if the byte array is null or empty.
        /// </summary>
        public static bool IsNullOrEmpty(byte[] bytes)
        {
            return bytes == null || bytes.Length == 0;
        }
        
        /// <summary>
        /// Converts from a UTF-8 encoded byte array to a string.
        /// </summary>
        public static string ByteArraysToString(byte[] b)
        {
            return Encoding.UTF8.GetString(b);
        }

        /// <summary>
        /// Performs a deep copy of a byte array.
        /// </summary>
        public static byte[] CopyBytesFrom(byte[] from)
        {
            int length = Buffer.ByteLength(from);
            byte[] to = new byte[length];
            Buffer.BlockCopy(from, 0, to, 0, length);
            return to;
        }

        /// <summary>
        /// Serializes object to a byte array with a <see cref="BinaryFormatter"/>.
        /// </summary>
        /// <exception cref="SerializationException">When serialization fails.</exception>
        public static byte[] SerializeToBinaryFormat(object obj)
        {
            using (var memStream = new MemoryStream())
            {
                new BinaryFormatter().Serialize(memStream, obj);
                return memStream.ToArray();
            }
        }

        /// <summary>
        /// Deserializes object from a byte array with a <see cref="BinaryFormatter"/>.
        /// </summary>
        /// <exception cref="SerializationException">When deserialization fails.</exception>
        public static object DeserializeFromBinaryFormat(byte[] bytes)
        {
            var formatter = new BinaryFormatter();
            using (var memStream = new MemoryStream(bytes))
            {
                return formatter.Deserialize(memStream);
            }
        }
    }
}
