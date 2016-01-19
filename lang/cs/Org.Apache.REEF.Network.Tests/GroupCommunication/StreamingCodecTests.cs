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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Wake.StreamingCodec;
using Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs;
using Xunit;

namespace Org.Apache.REEF.Network.Tests.GroupCommunication
{
    /// <summary>
    /// Defines streaming codec tests
    /// </summary>
    public class StreamingCodecTests
    {
        [Fact]
        public async Task TestCommonStreamingCodecs()
        {
            IInjector injector = TangFactory.GetTang().NewInjector();
            IStreamingCodec<int> intCodec = injector.GetInstance<IntStreamingCodec>();
            IStreamingCodec<double> doubleCodec = injector.GetInstance<DoubleStreamingCodec>();
            IStreamingCodec<float> floatCodec = injector.GetInstance<FloatStreamingCodec>();

            IStreamingCodec<int[]> intArrCodec = injector.GetInstance<IntArrayStreamingCodec>();
            IStreamingCodec<double[]> doubleArrCodec = injector.GetInstance<DoubleArrayStreamingCodec>();
            IStreamingCodec<float[]> floatArrCodec = injector.GetInstance<FloatArrayStreamingCodec>();

            IStreamingCodec<string> stringCodec = injector.GetInstance<StringStreamingCodec>();

            CancellationToken token = new CancellationToken();

            int obj = 5;
            int[] intArr = { 1, 2 };
            double[] doubleArr = { 1, 2 };
            float[] floatArr = { 1, 2 };
            string stringObj = "hello";

            var stream = new MemoryStream();
            IDataWriter writer = new StreamDataWriter(stream);
            intCodec.Write(obj, writer);
            await intCodec.WriteAsync(obj + 1, writer, token);
            doubleCodec.Write(obj + 2, writer);
            await doubleCodec.WriteAsync(obj + 3, writer, token);
            floatCodec.Write(obj + 4, writer);
            await floatCodec.WriteAsync(obj + 5, writer, token);
            intArrCodec.Write(intArr, writer);
            await intArrCodec.WriteAsync(intArr, writer, token);
            doubleArrCodec.Write(doubleArr, writer);
            await doubleArrCodec.WriteAsync(doubleArr, writer, token);
            floatArrCodec.Write(floatArr, writer);
            await floatArrCodec.WriteAsync(floatArr, writer, token);
            stringCodec.Write(stringObj, writer);
            await stringCodec.WriteAsync(stringObj, writer, token);

            stream.Position = 0;
            IDataReader reader = new StreamDataReader(stream);
            int res1 = intCodec.Read(reader);
            int res2 = await intCodec.ReadAsync(reader, token);
            double res3 = doubleCodec.Read(reader);
            double res4 = await doubleCodec.ReadAsync(reader, token);
            float res5 = floatCodec.Read(reader);
            float res6 = await floatCodec.ReadAsync(reader, token);
            int[] resArr1 = intArrCodec.Read(reader);
            int[] resArr2 = await intArrCodec.ReadAsync(reader, token);
            double[] resArr3 = doubleArrCodec.Read(reader);
            double[] resArr4 = await doubleArrCodec.ReadAsync(reader, token);
            float[] resArr5 = floatArrCodec.Read(reader);
            float[] resArr6 = await floatArrCodec.ReadAsync(reader, token);
            string resArr7 = stringCodec.Read(reader);
            string resArr8 = await stringCodec.ReadAsync(reader, token);

            Assert.Equal(obj, res1);
            Assert.Equal(obj + 1, res2);
            Assert.Equal(obj + 2, res3);
            Assert.Equal(obj + 3, res4);
            Assert.Equal(obj + 4, res5);
            Assert.Equal(obj + 5, res6);
            Assert.Equal(stringObj, resArr7);
            Assert.Equal(stringObj, resArr8);

            for (int i = 0; i < intArr.Length; i++)
            {
                Assert.Equal(intArr[i], resArr1[i]);
                Assert.Equal(intArr[i], resArr2[i]);
            }

            for (int i = 0; i < doubleArr.Length; i++)
            {
                Assert.Equal(doubleArr[i], resArr3[i]);
                Assert.Equal(doubleArr[i], resArr4[i]);
            }

            for (int i = 0; i < floatArr.Length; i++)
            {
                Assert.Equal(floatArr[i], resArr5[i]);
                Assert.Equal(floatArr[i], resArr6[i]);
            }
        }

        [Fact]
        public void TestIntArrayStreamingCodecsNullException()
        {
            IInjector injector = TangFactory.GetTang().NewInjector();
            IStreamingCodec<int[]> intArrCodec = injector.GetInstance<IntArrayStreamingCodec>();
            var stream = new MemoryStream();
            IDataWriter writer = new StreamDataWriter(stream);
            Assert.Throws<ArgumentNullException>(() => intArrCodec.Write(null, writer));
        }

        [Fact]
        public void TestFloatArrayStreamingCodecsNullException()
        {
            IInjector injector = TangFactory.GetTang().NewInjector();
            IStreamingCodec<float[]> floatArrCodec = injector.GetInstance<FloatArrayStreamingCodec>();
            var stream = new MemoryStream();
            IDataWriter writer = new StreamDataWriter(stream);
            Assert.Throws<ArgumentNullException>(() => floatArrCodec.Write(null, writer));
        }

        [Fact]
        public void TestDoubleArrayStreamingCodecsNullException()
        {
            IInjector injector = TangFactory.GetTang().NewInjector();
            IStreamingCodec<double[]> doubleArrCodec = injector.GetInstance<DoubleArrayStreamingCodec>();
            var stream = new MemoryStream();
            IDataWriter writer = new StreamDataWriter(stream);
            Assert.Throws<ArgumentNullException>(() => doubleArrCodec.Write(null, writer));
        }

        [Fact]
        public void TestNullStreamException()
        {
            IDataWriter writer;
            Assert.Throws<ArgumentNullException>(() => writer = new StreamDataWriter(null));
        }

        [Fact]
        public async Task TestCodecToStreamingCodec()
        {
            var config = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<ICodec<int>>.Class, GenericType<IntCodec>.Class)
                .BindImplementation(GenericType<IStreamingCodec<int>>.Class,
                    GenericType<CodecToStreamingCodec<int>>.Class)
                .Build();

            IStreamingCodec<int> streamingCodec =
                TangFactory.GetTang().NewInjector(config).GetInstance<IStreamingCodec<int>>();
           
            CancellationToken token = new CancellationToken();

            int obj = 5;
            var stream = new MemoryStream();
            IDataWriter writer = new StreamDataWriter(stream);
            streamingCodec.Write(obj, writer);
            await streamingCodec.WriteAsync(obj + 1, writer, token);

            stream.Position = 0;
            IDataReader reader = new StreamDataReader(stream);
            int res1 = streamingCodec.Read(reader);
            int res2 = await streamingCodec.ReadAsync(reader, token);
            Assert.Equal(obj, res1);
            Assert.Equal(obj + 1, res2);
        }
    }
}
