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
    /// Codec for the double array
    /// </summary>
    public class DoubleArrayCodec : ICodec<double[]>
    {
        [Inject]
        public DoubleArrayCodec()
        {
        }

        /// <summary>
        /// Converts the double array to the byte array
        /// </summary>
        /// <param name="obj"> The double array to be encoded</param>
        /// <returns>The byte array encoding</returns>
        public byte[] Encode(double[] obj)
        {
            byte[] result = new byte[sizeof(double) * obj.Length];
            Buffer.BlockCopy(obj, 0, result, 0, result.Length);
            return result;
        }

        /// <summary>
        /// Converts the byte array to the double array
        /// </summary>
        /// <param name="data"> The byte array to be decoded</param>
        /// <returns>The decoded double array</returns>
        public double[] Decode(byte[] data)
        {
            if (data.Length % sizeof(double) != 0)
            {
                throw new Exception("error inside double array decoder, byte array length not a multiple of interger size");
            }

            double[] result = new double[data.Length / sizeof(double)];
            Buffer.BlockCopy(data, 0, result, 0, data.Length);
            return result;
        }
    }
}
