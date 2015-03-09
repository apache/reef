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

using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Common.Avro;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Tests
{
    /// <summary>
    /// Class TestHttpSerialization. This class contains unit tests for HttpRequest serailization and deserialization.
    /// </summary>
    [TestClass]
    public class TestHttpSerialization
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(TestHttpSerialization));

        /// <summary>
        /// Tests the HTTP request serialization jason round trip.
        /// </summary>
        [TestMethod]
        public void TestHttpRequestSerializationJasonRoundTrip()
        {
            AvroHttpRequest r = CreatAvorHttpRequest();

            string str = AvroHttpSerializer.ToJson(r);
            byte[] bytes = ByteUtilities.StringToByteArrays(str);
            var r1 = AvroHttpSerializer.FromBytesWithJoson(bytes);

            var ri = ByteUtilities.ByteArrarysToString(r.InputStream);
            var ri1 = ByteUtilities.ByteArrarysToString(r1.InputStream);
            Assert.AreEqual(ri, ri1);
            Assert.AreEqual(r.QueryString, r1.QueryString);
        }

        /// <summary>
        /// Tests the HTTP request serialization round trip.
        /// </summary>
        [TestMethod]
        public void TestHttpRequestSerializationBytesRoundTrip()
        {
            AvroHttpRequest r = CreatAvorHttpRequest();

            var b = AvroHttpSerializer.ToBytes(r);
            var r1 = AvroHttpSerializer.FromBytes(b);

            var ri = ByteUtilities.ByteArrarysToString(r.InputStream);
            var ri1 = ByteUtilities.ByteArrarysToString(r1.InputStream);
            Assert.AreEqual(ri, ri1);
            Assert.AreEqual(r.QueryString, r1.QueryString);
        }

        /// <summary>
        /// Creats an Avor HTTP request for unit tests.
        /// </summary>
        /// <returns>AvroHttpRequest.</returns>
        private AvroHttpRequest CreatAvorHttpRequest()
        {
            AvroHttpRequest r = new AvroHttpRequest();
            r.Header = new List<HeaderEntry>();
            HeaderEntry he1 = new HeaderEntry();
            he1.key = "a";
            he1.value = "xxx";
            HeaderEntry he2 = new HeaderEntry();
            he2.key = "b";
            he2.value = "yyy";
            r.Header.Add(he1);
            r.Header.Add(he2);

            r.HttpMethod = "POST";
            r.InputStream = ByteUtilities.StringToByteArrays("test binary stream data");
            r.PathInfo = "/reef/evaluators";
            r.QueryString = "id=12&id=34&a=b";
            r.RequestUrl = "http://localhost:8080/reef/evaluators?id=12&id=34&a=b";
            return r;
        }
    }
}
