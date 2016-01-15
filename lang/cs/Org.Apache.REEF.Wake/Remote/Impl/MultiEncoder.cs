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
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Proto;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// Encoder using the WakeTuple protocol buffer
    /// (class name and bytes)
    /// </summary>
    public class MultiEncoder<T> : IEncoder<T>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MultiEncoder<>));
        private readonly Dictionary<Type, object> _encoderMap;
        private readonly Dictionary<Type, string> _nameMap;

        /// <summary>
        /// Constructs an encoder that encodes an object to bytes based on the class name
        /// </summary>
        public MultiEncoder()
        {
            _encoderMap = new Dictionary<Type, object>();
            _nameMap = new Dictionary<Type, string>();
        }

        public void Register<U>(IEncoder<U> encoder) where U : T
        {
            _encoderMap[typeof(U)] = encoder;
            _nameMap[typeof(U)] = typeof(U).ToString();
        }

        public void Register<U>(IEncoder<U> encoder, string name) where U : T
        {
            _encoderMap[typeof(U)] = encoder;
            _nameMap[typeof(U)] = name;
            Logger.Log(Level.Verbose, "Registering name for " + name);
        }

        /// <summary>Encodes an object to a byte array</summary>
        /// <param name="obj"></param>
        public byte[] Encode(T obj)
        {
            // Find encoder for object type
            object encoder;
            if (!_encoderMap.TryGetValue(obj.GetType(), out encoder))
            {
                return null;
            }

            // Invoke encoder for this type
            Type handlerType = typeof(IEncoder<>).MakeGenericType(new[] { obj.GetType() });
            MethodInfo info = handlerType.GetMethod("Encode");
            byte[] data = (byte[])info.Invoke(encoder, new[] { (object)obj });

            // Serialize object type and object data into well known tuple
            // To decode, deserialize the tuple, get object type, and look up the
            // decoder for that type
            string name = _nameMap[obj.GetType()];
            Logger.Log(Level.Verbose, "Encoding name for " + name);
            WakeTuplePBuf pbuf = new WakeTuplePBuf { className = name, data = data };
            pbuf.className = name;
            pbuf.data = data; 
            return pbuf.Serialize();
        }
    }
}
