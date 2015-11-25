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
using System.Text;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Xunit;

namespace Org.Apache.REEF.Wake.Tests
{
    public class MultiCodecTest
    {
        [Fact]
        public void TestMultiCodec()
        {
            MultiCodec<BaseEvent> codec = new MultiCodec<BaseEvent>();
            codec.Register(new Event1Codec());
            codec.Register(new Event2Codec());

            byte[] d1Data = codec.Encode(new Event1(42));
            byte[] d2Data = codec.Encode(new Event2("Tony"));

            Event1 e1 = (Event1)codec.Decode(d1Data);
            Event2 e2 = (Event2)codec.Decode(d2Data);

            Assert.Equal(42, e1.Number);
            Assert.Equal("Tony", e2.Name);
        }

        private class BaseEvent
        {
        }

        private class Event1 : BaseEvent
        {
            public Event1(int number)
            {
                Number = number;
            }

            public int Number { get; set; }
        }

        private class Event2 : BaseEvent
        {
            public Event2(string name)
            {
                Name = name;
            }

            public string Name { get; set; }
        }

        private class Event1Codec : ICodec<Event1>
        {
            public byte[] Encode(Event1 obj)
            {
                return BitConverter.GetBytes(obj.Number);
            }

            public Event1 Decode(byte[] data)
            {
                return new Event1(BitConverter.ToInt32(data, 0));
            }
        }

        private class Event2Codec : ICodec<Event2>
        {
            public byte[] Encode(Event2 obj)
            {
                return Encoding.ASCII.GetBytes(obj.Name);
            }

            public Event2 Decode(byte[] data)
            {
                return new Event2(Encoding.ASCII.GetString(data));
            }
        }
    }
}
