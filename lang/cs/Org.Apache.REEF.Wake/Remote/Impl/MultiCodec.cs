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

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// Codec that can encode and decode a class depending on the class type.
    /// </summary>
    public class MultiCodec<T> : ICodec<T>
    {
        private readonly MultiEncoder<T> _encoder;

        private readonly MultiDecoder<T> _decoder;

        /// <summary>
        /// Constructs a new MultiCodec object.
        /// </summary>
        public MultiCodec()
        {
            _encoder = new MultiEncoder<T>();
            _decoder = new MultiDecoder<T>();
        }

        /// <summary>
        /// Register a codec to be used when encoding/decoding objects of this type.
        /// </summary>
        /// <typeparam name="U">The type of codec</typeparam>
        /// <param name="codec">The codec to use when encoding/decoding
        /// objects of this type</param>
        public void Register<U>(ICodec<U> codec) where U : T
        {
            _encoder.Register(codec);
            _decoder.Register(codec);
        }

        /// <summary>
        /// Register a codec to be used when encoding/decoding objects of this type.
        /// </summary>
        /// <typeparam name="U">The type of codec</typeparam>
        /// <param name="codec">The codec to use when encoding/decoding
        /// objects of this type</param>
        /// <param name="name">The name of the class to encode/decode</param>
        public void Register<U>(ICodec<U> codec, string name) where U : T
        {
            _encoder.Register(codec, name);
            _decoder.Register(codec, name);
        }

        /// <summary>
        /// Encodes an object with the appropriate encoding or null if it cannot
        /// be encoded.
        /// </summary>
        /// <param name="obj">Data to encode</param>
        public byte[] Encode(T obj)
        {
            return _encoder.Encode(obj);
        }

        /// <summary>
        /// Decodes byte array into the appropriate object type.
        /// </summary>
        /// <param name="data">Data to be decoded</param>
        public T Decode(byte[] data)
        {
            return _decoder.Decode(data);
        }
    }
}
