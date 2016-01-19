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
using System.Reflection;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Proto;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// Decoder using the WakeTuple protocol buffer
    /// (class name and bytes)
    /// </summary>
    public class MultiDecoder<T> : IDecoder<T>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(MultiDecoder<T>));
        private readonly Dictionary<Type, object> _decoderMap;
        private readonly Dictionary<string, Type> _nameMap;

        /// <summary>
        /// Constructs a decoder that decodes bytes based on the class type
        /// </summary>
        public MultiDecoder()
        {
            _decoderMap = new Dictionary<Type, object>();
            _nameMap = new Dictionary<string, Type>();
        }

        /// <summary>
        /// Register the decoder for objects of type U
        /// </summary>
        /// <typeparam name="U">The type of decoder to use when decoding
        /// objects of this type</typeparam>
        /// <param name="decoder">The decoder to use when decoding
        /// objects of this type</param>
        public void Register<U>(IDecoder<U> decoder) where U : T
        {
            Type type = typeof(U);
            _decoderMap[type] = decoder;
            _nameMap[type.ToString()] = type;
        }

        /// <summary>
        /// Register the decoder for objects of type U
        /// </summary>
        /// <typeparam name="U">The type of decoder to use when decoding
        /// objects of this type</typeparam>
        /// <param name="decoder">The decoder to use when decoding
        /// objects of this type</param>
        /// <param name="name">The name of the class to decode</param>
        public void Register<U>(IDecoder<U> decoder, string name) where U : T
        {
            Type type = typeof(U);
            _decoderMap[type] = decoder;
            _nameMap[name] = type;
        }

        /// <summary>
        /// Decodes byte array according to the underlying object type.
        /// </summary>
        /// <param name="data">The data to decode</param>
        public T Decode(byte[] data)
        {
            WakeTuplePBuf pbuf = WakeTuplePBuf.Deserialize(data);
            if (pbuf == null)
            {
                return default(T);
            }

            // Get object's class Type
            Type type;
            if (!_nameMap.TryGetValue(pbuf.className, out type))
            {
                return default(T);
            }

            // Get decoder for that type
            object decoder;
            if (!_decoderMap.TryGetValue(type, out decoder))
            {
                Exceptions.Throw(new RemoteRuntimeException("Decoder for " + type + " not known."), LOGGER);
            }

            // Invoke the decoder to decode the byte array
            Type handlerType = typeof(IDecoder<>).MakeGenericType(new[] { type });
            MethodInfo info = handlerType.GetMethod("Decode");
            return (T)info.Invoke(decoder, new[] { (object)pbuf.data });
        }
    }
}
