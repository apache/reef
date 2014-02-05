/**
 * Copyright (C) 2012 Microsoft Corporation
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
package com.microsoft.wake.remote.transport.netty;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.DownstreamMessageEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.UpstreamMessageEvent;
import org.jboss.netty.handler.stream.ChunkedStream;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;

/**
 * Thin wrapper around ChunkedWriteHandler
 * 
 * ChunkedWriteHandler only handles the down stream parts
 * and just emits the chunks up stream. So we add an upstream
 * handler that aggregates the chunks into its original form. This
 * is guaranteed to be thread serial so state can be shared. 
 * 
 * On the down stream side, we just decorate the original message
 * with its size and allow the thread-serial base class to actually
 * handle the chunking. We need to be careful since the decoration
 * itself has to be thread-safe since netty does not guarantee thread
 * serial access to down stream handlers.
 * 
 * We do not need to tag the writes since the base class ChunkedWriteHandler
 * serializes access to the channel and first write will complete before
 * the second begins.
 *
 */
public class ChunkedReadWriteHandler extends ChunkedWriteHandler {

  public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

  private static final Logger LOG = Logger.getLogger(ChunkedReadWriteHandler.class.getName());

  private boolean start = true;
  private int expectedSize = 0;

  private ChannelBuffer readBuffer;

  private byte[] retArr;
  
  /**
   * @see org.jboss.netty.handler.stream.ChunkedWriteHandler#handleUpstream(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ChannelEvent)
   */
  @Override
  public void handleUpstream(final ChannelHandlerContext ctx, final ChannelEvent chEvent) throws Exception {
    if (chEvent instanceof MessageEvent) {
      String curThrName = Thread.currentThread().getName() + ": ";
      final MessageEvent msgEvent = (MessageEvent) chEvent;
      final byte[] data = (byte[]) msgEvent.getMessage();

      if (null == data || data.length == 0) {
        super.handleUpstream(ctx, chEvent);
        return;
      }

      if (start) {
        //LOG.log(Level.FINEST, "{0} Starting dechunking of a chunked write", curThrName);
        expectedSize = getSize(data);
        //if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, curThrName + "Expected Size = {0}. Wrapping byte[{1}] into a ChannelBuffer", new Object[]{expectedSize,expectedSize});
        retArr = new byte[expectedSize];
        readBuffer = ChannelBuffers.wrappedBuffer(retArr);
        readBuffer.clear();
        //if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, curThrName + "read buffer: cur sz = " + readBuffer.writerIndex() + " + " + (data.length - INT_SIZE) + " bytes will added by current chunk");
        readBuffer.writeBytes(data, INT_SIZE, data.length - INT_SIZE);
        //if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, curThrName + "read buffer: new sz = " + readBuffer.writerIndex());
        start = false;
      } else {
        //if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, curThrName + "In the middle of dechunking of a chunked write");
        //if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, curThrName + "read buffer: cur sz = " + readBuffer.writerIndex() + " + " + data.length + " bytes will added by current chunk");
        readBuffer.writeBytes(data);
        //if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, curThrName + "read buffer: new sz = " + readBuffer.writerIndex());
      }

      if (readBuffer.writerIndex() == expectedSize) {
        //if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, "{0} Dechunking complete. Creating upstream msg event with the dechunked byte[{1}]", new Object[]{curThrName, expectedSize});
        final MessageEvent ret = new UpstreamMessageEvent(
            msgEvent.getChannel(), retArr, msgEvent.getRemoteAddress());
        //if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, "Resetting state to begin another dechunking", curThrName);
        start = true;
        expectedSize = 0;
        readBuffer = null;
        retArr = null;
        //LOG.log(Level.FINEST, "{0} Sending dechunked message upstream", curThrName);
        super.handleUpstream(ctx, ret);
      }
    } else {
      super.handleUpstream(ctx, chEvent);
    }
  }

  /**
   * Thread-safe since there is no shared instance state.
   * Just prepend size to the message and stream it through
   * a chunked stream and let the base method handle the actual
   * chunking.
   * 
   * We do not need to tag the writes since the base class ChunkedWriteHandler
   * serializes access to the channel and first write will complete before
   * the second begins.
   */
  @Override
  public void handleDownstream(final ChannelHandlerContext ctx, final ChannelEvent event) throws Exception {

    if (event instanceof MessageEvent) {

      final MessageEvent msgEvent = (MessageEvent) event;
      final Object msg = msgEvent.getMessage();

      if (msg instanceof byte[]) {
        String curThrName = Thread.currentThread().getName() + ": ";
        final byte[] data = (byte[]) msg;
        final byte[] size = sizeAsByteArr(data.length);
        //if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, "{0} Setting size={1}", new Object[] {curThrName, data.length});
        final ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(size, data);
        final ChannelBufferInputStream stream = new ChannelBufferInputStream(buffer);

        final ChunkedStream chunkedStream = new ChunkedStream(
            stream, NettyChannelPipelineFactory.MAXFRAMELENGTH - 1024);
        //if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, "{0} Created chunkedstream {1}", new Object[]{curThrName, chunkedStream});
        final MessageEvent dwnStrmMsgEvent = new DownstreamMessageEvent(
            msgEvent.getChannel(), msgEvent.getFuture(), chunkedStream,
            msgEvent.getRemoteAddress());
        //LOG.log(Level.FINEST, "{0} Sending downstream", curThrName);
        super.handleDownstream(ctx, dwnStrmMsgEvent);

      } else {
        super.handleDownstream(ctx, msgEvent);
      }

    } else {
      super.handleDownstream(ctx, event);
    }
  }

  /**
   * Converts the int size into a byte[]
   * @param size
   * @return the bit representation of size
   */
  private byte[] sizeAsByteArr(final int size) {
    final byte[] ret = new byte[INT_SIZE];
    final ChannelBuffer intBuffer =
        ChannelBuffers.wrappedBuffer(ChannelBuffers.LITTLE_ENDIAN, ret);
    intBuffer.clear();
    intBuffer.writeInt(size);
    return ret;
  }

  /**
   * Get expected size encoded as the first
   * 4 bytes of data
   * 
   * @param data
   * @return
   */
  private int getSize(final byte[] data) {
    return getSize(data, 0);
  }

  /**
   * Get expected size encoded as offset + 4 bytes 
   * of data
   * 
   * @param data
   * @param offset
   * @return
   */
  private int getSize(final byte[] data, final int offset) {
    if (data.length - offset < INT_SIZE) {
      return 0;
    } else {
      final ChannelBuffer intBuffer =
          ChannelBuffers.wrappedBuffer(ChannelBuffers.LITTLE_ENDIAN, data, offset, INT_SIZE);
      final int ret = intBuffer.readInt();
      return ret;
    }
  }
}
