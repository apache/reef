/*
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
package org.apache.reef.tests.group.conf;

import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tests.group.conf.GroupCommServiceInjectionDriver.GroupCommServiceInjectionParameter;

import javax.inject.Inject;
import java.nio.ByteBuffer;

/**
 * Group communication codec used in the GroupCommServiceInjection test.
 * Adds a certain offset value while decoding data.
 * The offset should be given via Tang injection.
 */
final class GroupCommServiceInjectionCodec implements Codec<Integer> {

  private final int offset;

  @Inject
  private GroupCommServiceInjectionCodec(@Parameter(GroupCommServiceInjectionParameter.class) final int offset) {
    this.offset = offset;
  }


  @Override
  public byte[] encode(final Integer integer) {
    final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
    byteBuffer.putInt(integer);
    return byteBuffer.array();
  }

  @Override
  public Integer decode(final byte[] data) {
    final ByteBuffer byteBuffer = ByteBuffer.wrap(data);
    return byteBuffer.getInt() + offset;
  }
}
