/**
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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.Group.CommonOperators.Driver.Codecs
{
    /// <summary>
    /// Codec for the int array
    /// </summary>
    public class IntegerArrayCodec : ICodec<int[]>
    {
        [Inject]
        public IntegerArrayCodec()
        {
        }

        /// <summary>
        /// Converts the int array in to the byte array
        /// </summary>
        /// <param name="obj"> The int array to be encoded</param>
        /// <returns>The byte array encoding</returns>
        public byte[] Encode(int[] obj)
        {
            byte[] result = new byte[sizeof(Int32) * obj.Length];
            Buffer.BlockCopy(obj, 0, result, 0, result.Length);
            return result;
        }

        /// <summary>
        /// Converts the byte array to the int array
        /// </summary>
        /// <param name="data"> The byte array to be decoded</param>
        /// <returns>The decoded int array</returns>
        public int[] Decode(byte[] data)
        {
            if (data.Length % sizeof(Int32) != 0)
            {
                throw new Exception("error inside integer array decoder, byte array length not a multiple of interger size");
            }

            int[] result = new int[data.Length / sizeof(Int32)];
            Buffer.BlockCopy(data, 0, result, 0, data.Length);
            return result;
        }
    }
}
