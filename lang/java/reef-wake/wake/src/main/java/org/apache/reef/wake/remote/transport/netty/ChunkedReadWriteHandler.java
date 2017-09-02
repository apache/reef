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
package org.apache.reef.wake.remote.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.stream.ChunkedStream;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Thin wrapper around ChunkedWriteHandler.
 * <p>
 * ChunkedWriteHandler only handles the down stream parts
 * and just emits the chunks up stream. So we add an upstream
 * handler that aggregates the chunks into its original form. This
 * is guaranteed to be thread serial so state can be shared.
 * <p>
 * On the down stream side, we just decorate the original message
 * with its size and allow the thread-serial base class to actually
 * handle the chunking. We need to be careful since the decoration
 * itself has to be thread-safe since netty does not guarantee thread
 * serial access to down stream handlers.
 * <p>
 * We do not need to tag the writes since the base class ChunkedWriteHandler
 * serializes access to the channel and first write will complete before
 * the second begins.
 */
public class ChunkedReadWriteHandler extends ChunkedWriteHandler {

  public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

  private static final Logger LOG = Logger.getLogger(ChunkedReadWriteHandler.class.getName());

  private boolean start = true;
  private int expectedSize = 0;

  private ByteBuf readBuffer;
  private byte[] retArr;

  /**
   * @see org.jboss.netty.handler.stream.ChunkedWriteHandler#handleUpstream(
   *      org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ChannelEvent)
   */
  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {

    if (msg instanceof byte[]) {

      final byte[] data = (byte[]) msg;

      if (start) {
        //LOG.log(Level.FINEST, "{0} Starting dechunking of a chunked write", curThrName);
        expectedSize = getSize(data);
        // LOG.log(Level.FINEST, "Expected Size = {0}. Wrapping byte[{1}] into a ChannelBuffer",
        // new Object[]{expectedSize,expectedSize});
        retArr = new byte[expectedSize];
        readBuffer = Unpooled.wrappedBuffer(retArr);
        readBuffer.clear();
        //if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, curThrName + "read buffer: cur sz = " +
        // readBuffer.writerIndex() + " + " + (data.length - INT_SIZE) + " bytes will added by current chunk");
        readBuffer.writeBytes(data, INT_SIZE, data.length - INT_SIZE);
        //if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, curThrName + "read buffer: new sz = " +
        // readBuffer.writerIndex());
        start = false;
      } else {
        readBuffer.writeBytes(data);
      }

      if (readBuffer.writerIndex() == expectedSize) {
        //if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, "{0} Dechunking complete." +
        // "Creating upstream msg event with the dechunked byte[{1}]", new Object[]{curThrName, expectedSize});
        //if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, "Resetting state to begin another dechunking",
        // curThrName);
        final byte[] temp = retArr;
        start = true;
        expectedSize = 0;
        readBuffer.release();
        retArr = null;
        //LOG.log(Level.FINEST, "{0} Sending dechunked message upstream", curThrName);
        super.channelRead(ctx, temp);
      }
    } else {
      super.channelRead(ctx, msg);
    }
  }

  /**
   * Thread-safe since there is no shared instance state.
   * Just prepend size to the message and stream it through
   * a chunked stream and let the base method handle the actual
   * chunking.
   * <p>
   * We do not need to tag the writes since the base class ChunkedWriteHandler
   * serializes access to the channel and first write will complete before
   * the second begins.
   */
  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {

    if (msg instanceof ByteBuf) {

      final ByteBuf bf = (ByteBuf) msg;

      if (bf.hasArray()) {
        final byte[] data = bf.array();
        final byte[] size = sizeAsByteArr(data.length);
        final ByteBuf writeBuffer = Unpooled.wrappedBuffer(size, data);
        final ByteBufCloseableStream stream = new ByteBufCloseableStream(writeBuffer);
        final ChunkedStream chunkedStream = new ChunkedStream(
            stream, NettyChannelInitializer.MAXFRAMELENGTH - 1024);
        super.write(ctx, chunkedStream, promise);
      } else {
        super.write(ctx, msg, promise);
      }

    } else {
      super.write(ctx, msg, promise);
    }
  }

  /**
   * Converts the int size into a byte[].
   *
   * @return the bit representation of size
   */
  private byte[] sizeAsByteArr(final int size) {
    final byte[] ret = new byte[INT_SIZE];
    final ByteBuf intBuffer = Unpooled.wrappedBuffer(ret).order(Unpooled.LITTLE_ENDIAN);
    intBuffer.clear();
    intBuffer.writeInt(size);
    intBuffer.release();
    return ret;
  }

  /**
   * Get expected size encoded as the first 4 bytes of data.
   */
  private int getSize(final byte[] data) {
    return getSize(data, 0);
  }

  /**
   * Get expected size encoded as offset + 4 bytes of data.
   */
  private int getSize(final byte[] data, final int offset) {

    if (data.length - offset < INT_SIZE) {
      return 0;
    }

    final ByteBuf intBuffer = Unpooled.wrappedBuffer(data, offset, INT_SIZE).order(Unpooled.LITTLE_ENDIAN);
    final int ret = intBuffer.readInt();
    intBuffer.release();

    return ret;
  }

  /**
   * Release Bytebuf when the stream closes.
   */
  private class ByteBufCloseableStream extends ByteBufInputStream {
    private final ByteBuf buffer;

    ByteBufCloseableStream(final ByteBuf buffer) {
      super(buffer);
      this.buffer = buffer;
    }

    @Override
    public void close() throws IOException {
      super.close();
      buffer.release();
    }
  }
}
