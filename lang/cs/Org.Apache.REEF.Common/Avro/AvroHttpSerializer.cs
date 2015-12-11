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
using System.IO;
using Microsoft.Hadoop.Avro;
using Newtonsoft.Json;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Common.Avro
{
    /// <summary>
    /// Class AvroHttpSerializer. Provides methods to serialize and deserialize HttpRequest
    /// </summary>
    //// TODO[REEF-842] Act on the obsoletes
    public class AvroHttpSerializer
    {
        public static AvroHttpRequest FromBytes(byte[] serializedBytes)
        {
            var serializer = AvroSerializer.Create<AvroHttpRequest>();
            using (var stream = new MemoryStream(serializedBytes))
            {
                return serializer.Deserialize(stream);
            }
        }

        /// <summary>
        /// Convert bytes which contains Json string into AvroHttpRequest object
        /// </summary>
        /// <param name="serializedBytes">The serialized bytes.</param>
        /// <returns>AvroHttpRequest.</returns>
        [Obsolete("Deprecated in 0.14, please use FromBytesWithJson instead.")]
        public static AvroHttpRequest FromBytesWithJoson(byte[] serializedBytes)
        {
            return FromBytesWithJson(serializedBytes);
        }

        /// <summary>
        /// Convert bytes which contains Json string into AvroHttpRequest object
        /// </summary>
        /// <param name="serializedBytes">The serialized bytes.</param>
        /// <returns>AvroHttpRequest.</returns>
        public static AvroHttpRequest FromBytesWithJson(byte[] serializedBytes)
        {
            string s = ByteUtilities.ByteArraysToString(serializedBytes);
            return FromJson(s);
        }

        /// <summary>
        /// Convert from Json string into AvroHttpRequest object
        /// </summary>
        /// <param name="jsonString">The json string.</param>
        /// <returns>AvroHttpRequest.</returns>
        public static AvroHttpRequest FromJson(string jsonString)
        {
           return JsonConvert.DeserializeObject<AvroHttpRequest>(jsonString);
        }

        /// <summary>
        /// Convert from AvroHttpRequest to Json string
        /// </summary>
        /// <param name="httpRequest">The HTTP request.</param>
        /// <returns>System.String.</returns>
        public static string ToJson(AvroHttpRequest httpRequest)
        {
            return JsonConvert.SerializeObject(httpRequest);
        }

        /// <summary>
        /// Serialize AvroHttpRequest object into bytes
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns>System.Byte[].</returns>
        public static byte[] ToBytes(AvroHttpRequest obj)
        {
            var serializer = AvroSerializer.Create<AvroHttpRequest>();
            using (MemoryStream stream = new MemoryStream())
            {
                serializer.Serialize(stream, obj);
                return stream.GetBuffer();
            }
        }

        /// <summary>
        /// Deserialize from file to AvroHttpRequest object.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <returns>AvroHttpRequest.</returns>
        public static AvroHttpRequest FromFile(string fileName)
        {
            var str = File.ReadAllText(fileName);
            AvroHttpRequest avroHttprequest = JsonConvert.DeserializeObject<AvroHttpRequest>(str);
            return avroHttprequest;
        }

        /// <summary>
        /// Serialize AvroHttpRequest in to file
        /// </summary>
        /// <param name="avroHttprequest">The avro httprequest.</param>
        /// <param name="fileName">Name of the file.</param>
        public static void ToFile(AvroHttpRequest avroHttprequest, string fileName)
        {
            var bytes = ToBytes(avroHttprequest);
            using (var file = File.OpenWrite(fileName))
            {
                file.Write(bytes, 0, bytes.Length);
            }
        }
    }
}
