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
package org.apache.reef.io.storage;

import org.apache.reef.exception.evaluator.ServiceException;
import org.apache.reef.exception.evaluator.StorageException;
import org.apache.reef.io.Accumulable;
import org.apache.reef.io.Accumulator;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class FramingOutputStream extends OutputStream implements Accumulable<byte[]> {

  private final ByteArrayOutputStream baos;
  private final DataOutputStream o;
  private long offset;
  private boolean closed;

  public FramingOutputStream(final OutputStream o) {
    if (!(o instanceof DataOutputStream)) {
      this.o = new DataOutputStream(o);
    } else {
      this.o = (DataOutputStream) o;
    }
    this.baos = new ByteArrayOutputStream();
  }

  public void nextFrame() throws IOException {
    o.writeInt(baos.size());
    offset += 4;
    baos.writeTo(o);
    baos.reset();
  }

  public long getCurrentOffset() {
    return offset;
  }

  @Override
  public void write(final int b) throws IOException {
    baos.write(b);
    offset++;
  }

  @Override
  public void write(final byte[] b) throws IOException {
    baos.write(b);
    this.offset += b.length;
  }

  @Override
  public void write(final byte[] b, final int offsetToWrite, final int length) throws IOException {
    baos.write(b, offsetToWrite, length);
    this.offset += length;
  }

  @Override
  public void flush() {
    // no-op.
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      if (this.offset > 0) {
        nextFrame();
      }
      o.writeInt(-1);
      o.close();
      closed = true;
    }
  }

  @Override
  public Accumulator<byte[]> accumulator() throws StorageException {
    @SuppressWarnings("resource")
    final FramingOutputStream fos = this;
    return new Accumulator<byte[]>() {

      @Override
      public void add(final byte[] datum) throws ServiceException {
        try {
          o.writeInt(datum.length);
          offset += 4;
          o.write(datum);
          offset += datum.length;
        } catch (final IOException e) {
          throw new ServiceException(e);
        }

      }

      @Override
      public void close() throws ServiceException {
        try {
          o.writeInt(-1);
          offset += 4;
          o.close();
          fos.closed = true;
        } catch (final IOException e) {
          throw new ServiceException(e);
        }
      }

    };
  }

}
