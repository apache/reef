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
package com.microsoft.reef.io.network.group.impl;

import com.google.protobuf.InvalidProtocolBufferException;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.wake.remote.Codec;

import javax.inject.Inject;

/**
 * Codec for {@link GroupCommMessage}
 */
public class GCMCodec implements Codec<GroupCommMessage> {

  @Inject
  public GCMCodec() {
  }

  @Override
  public GroupCommMessage decode(final byte[] data) {
    try {
      return GroupCommMessage.parseFrom(data);
    } catch (final InvalidProtocolBufferException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public byte[] encode(final GroupCommMessage msg) {
    return msg.toByteArray();
  }
}
