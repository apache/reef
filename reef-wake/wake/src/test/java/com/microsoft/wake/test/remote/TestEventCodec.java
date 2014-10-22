/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.wake.test.remote;

import com.microsoft.wake.test.proto.TestProtos.TestEventPBuf;

import com.google.protobuf.InvalidProtocolBufferException;
import com.microsoft.wake.remote.Codec;
import com.microsoft.wake.remote.exception.RemoteRuntimeException;

/**
 * TestEvent codec using the protocol buffer 
 */
public class TestEventCodec implements Codec<TestEvent> {

  @Override
  public byte[] encode(TestEvent obj) {
    TestEventPBuf.Builder builder = TestEventPBuf.newBuilder();
    builder.setMessage(obj.getMessage());
    builder.setLoad(obj.getLoad());
    return builder.build().toByteArray();
  }

  @Override
  public TestEvent decode(byte[] data) {
    TestEventPBuf pbuf;
    try {
      pbuf = TestEventPBuf.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      throw new RemoteRuntimeException(e);
    }

    return new TestEvent(pbuf.getMessage(), pbuf.getLoad());
  }

}
