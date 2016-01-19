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

using System.IO;
using Microsoft.Hadoop.Avro;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Utilities
{
    [Private]
    public static class AvroUtils
    {
        /// <summary>
        /// Convert an object to byte array using Avro serialization
        /// </summary>
        /// <param name="obj">The object to serialize</param>
        /// <returns>The serialized object in a byte array</returns>
        public static byte[] AvroSerialize<T>(T obj)
        {
            IAvroSerializer<T> serializer = AvroSerializer.Create<T>();
            using (MemoryStream stream = new MemoryStream())
            {
                serializer.Serialize(stream, obj);
                return stream.GetBuffer();
            }
        }

        /// <summary>
        /// Converts a byte array to an object using Avro deserialization.
        /// </summary>
        /// <param name="data">The byte array to deserialize</param>
        /// <returns>The deserialized object</returns>
        public static T AvroDeserialize<T>(byte[] data)
        {
            IAvroSerializer<T> deserializer = AvroSerializer.Create<T>();
            using (MemoryStream stream = new MemoryStream(data))
            {
                return deserializer.Deserialize(stream);
            }
        }
    }
}
