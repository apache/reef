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

using Newtonsoft.Json;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Avro
{
    /// <summary>
    /// Wrapper class for serialize/deserialize Avro json. This avoids having to reference Avro dll in every project 
    /// </summary>
    /// <typeparam name="T"> the deserialized type</typeparam>
    [Private]
    public sealed class AvroJsonSerializer<T>
    {
        public static T FromString(string str)
        {
            return JsonConvert.DeserializeObject<T>(str);
        }

        public static string ToString(T obj)
        {
            return JsonConvert.SerializeObject(obj);
        }

        public static T FromBytes(byte[] bytes)
        {
            return FromString(ByteUtilities.ByteArraysToString(bytes));
        }

        public static byte[] ToBytes(T obj)
        {
            return ByteUtilities.StringToByteArrays(JsonConvert.SerializeObject(obj));
        }
    }
}
