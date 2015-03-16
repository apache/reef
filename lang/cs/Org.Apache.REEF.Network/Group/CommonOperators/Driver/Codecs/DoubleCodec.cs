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
    /// Codec for the double
    /// </summary>
    public class DoubleCodec : ICodec<double>
    {
        [Inject]
        public DoubleCodec()
        {
        }

        /// <summary>
        /// Converts the double to the byte array
        /// </summary>
        /// <param name="obj"> The double to be encoded</param>
        /// <returns>The byte array encoding</returns>
        public byte[] Encode(double obj)
        {
            return BitConverter.GetBytes(obj);
        }

        /// <summary>
        /// Converts the byte array to the double
        /// </summary>
        /// <param name="data"> The byte array to be decoded</param>
        /// <returns>The decoded double</returns>
        public double Decode(byte[] data)
        {
            return BitConverter.ToDouble(data, 0);
        }
    }
}

