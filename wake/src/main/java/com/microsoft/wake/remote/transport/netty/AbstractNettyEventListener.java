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
package com.microsoft.wake.remote.transport.netty;

import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.TransportEvent;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Generic functionality for the Netty event listener.
 * This is a base class for client and server versions.
 */
abstract class AbstractNettyEventListener implements NettyEventListener {

  protected static final Logger LOG = Logger.getLogger(AbstractNettyEventListener.class.getName());

  protected final ConcurrentMap<SocketAddress, LinkReference> addrToLinkRefMap;
  protected final EStage<TransportEvent> stage;
  protected EventHandler<Exception> exceptionHandler;

  public AbstractNettyEventListener(
      final ConcurrentMap<SocketAddress, LinkReference> addrToLinkRefMap,
      final EStage<TransportEvent> stage) {
    this.addrToLinkRefMap = addrToLinkRefMap;
    this.stage = stage;
  }

  public void registerErrorHandler(final EventHandler<Exception> handler) {
    LOG.log(Level.FINE, "Set error handler {0}", handler);
    this.exceptionHandler = handler;
  }

  @Override
  public void messageReceived(final MessageEvent event) {

    final Channel channel = event.getChannel();
    final byte[] message = (byte[]) event.getMessage();

    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "MessageEvent: local: {0} remote: {1} :: {2}", new Object[] {
          channel.getLocalAddress(), channel.getRemoteAddress(), message });
    }

    if (message.length > 0) {
      // send to the dispatch stage
      this.stage.onNext(this.getTransportEvent(message, channel));
    }
  }

  @Override
  public void exceptionCaught(final ExceptionEvent event) {
    final Throwable ex = event.getCause();
    LOG.log(Level.WARNING, "ExceptionEvent: " + event, ex);
    this.exceptionCleanup(event);
    if (this.exceptionHandler != null) {
      this.exceptionHandler.onNext(ex instanceof Exception ? (Exception) ex : new Exception(ex));
    }
  }

  @Override
  public void channelClosed(final ChannelStateEvent event) {
    this.closeChannel(event.getChannel());
  }

  protected abstract TransportEvent getTransportEvent(final byte[] message, final Channel channel);

  protected abstract void exceptionCleanup(final ExceptionEvent event);

  protected void closeChannel(final Channel channel) {
    final LinkReference refRemoved =
        channel != null && channel.getRemoteAddress() != null ?
            this.addrToLinkRefMap.remove(channel.getRemoteAddress()) : null;
    LOG.log(Level.FINER, "Channel closed: {0}. Link ref found and removed: {1}",
        new Object[]{channel, refRemoved != null});
  }
}
