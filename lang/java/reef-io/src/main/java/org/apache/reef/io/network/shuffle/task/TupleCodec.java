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
package org.apache.reef.io.network.shuffle.task;

import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.network.shuffle.params.ShuffleKeyCodec;
import org.apache.reef.io.network.shuffle.params.ShuffleValueCodec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 */
public final class TupleCodec<K, V> implements StreamingCodec<Tuple<K, V>> {

  private final Codec<K> keyCodec;
  private final Codec<V> valueCodec;

  @Inject
  public TupleCodec(
      final @Parameter(ShuffleKeyCodec.class) Codec<K> keyCodec,
      final @Parameter(ShuffleValueCodec.class) Codec<V> valueCodec) {
    this.keyCodec = keyCodec;
    this.valueCodec = valueCodec;
  }

  @Override
  public byte[] encode(final Tuple<K, V> tuple) {
    final byte[] keyData = keyCodec.encode(tuple.getKey());
    final byte[] valueData = valueCodec.encode(tuple.getValue());
    final ByteBuffer buffer = ByteBuffer.allocate(8 + keyData.length + valueData.length);
    buffer.putInt(keyData.length);
    buffer.put(keyData);
    buffer.putInt(valueData.length);
    buffer.put(valueData);
    return buffer.array();
  }

  @Override
  public Tuple<K, V> decode(final byte[] data) {
    final ByteBuffer buffer = ByteBuffer.wrap(data);
    final byte[] keyData = new byte[buffer.getInt()];
    buffer.get(keyData);
    final byte[] valueData = new byte[buffer.getInt()];
    buffer.get(valueData);
    return new Tuple<>(keyCodec.decode(keyData), valueCodec.decode(valueData));
  }

  @Override
  public void encodeToStream(final Tuple<K, V> tuple, final DataOutputStream stream) {
    try {
      if (keyCodec instanceof StreamingCodec) {
        ((StreamingCodec<K>) keyCodec).encodeToStream(tuple.getKey(), stream);
      } else {
        final byte[] keyData = keyCodec.encode(tuple.getKey());
        stream.writeInt(keyData.length);
        stream.write(keyData);
      }

      if (valueCodec instanceof StreamingCodec) {
        ((StreamingCodec<V>) valueCodec).encodeToStream(tuple.getValue(), stream);
      } else {
        final byte[] valueData = valueCodec.encode(tuple.getValue());
        stream.writeInt(valueData.length);
        stream.write(valueData);
      }
    } catch (final IOException e) {
      throw new RuntimeException("An IOException occurred while encoding tuple with codecs [ key : "
          + keyCodec + ", value : " + valueCodec + " ]", e);
    }
  }

  @Override
  public Tuple<K, V> decodeFromStream(final DataInputStream stream) {
    try {
      final K key;
      if (keyCodec instanceof StreamingCodec) {
        key = ((StreamingCodec<K>) keyCodec).decodeFromStream(stream);
      } else {
        final int keyDatLength = stream.readInt();
        final byte[] keyData = new byte[keyDatLength];
        stream.readFully(keyData);
        key = keyCodec.decode(keyData);
      }

      final V value;
      if (valueCodec instanceof StreamingCodec) {
        value = ((StreamingCodec<V>) valueCodec).decodeFromStream(stream);
      } else {
        final int valueDatLength = stream.readInt();
        final byte[] valueData = new byte[valueDatLength];
        stream.readFully(valueData);
        value = valueCodec.decode(valueData);
      }
      return new Tuple<>(key, value);
    } catch (final IOException e) {
      throw new RuntimeException("An IOException occurred while encoding tuple with codecs [ key : "
          + keyCodec + ", value : " + valueCodec + " ]", e);
    }
  }
}
