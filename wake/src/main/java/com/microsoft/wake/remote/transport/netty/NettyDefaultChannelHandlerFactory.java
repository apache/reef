package com.microsoft.wake.remote.transport.netty;

import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;

/**
 * Default Netty channel handler factory
 */
class NettyDefaultChannelHandlerFactory implements NettyChannelHandlerFactory {

  /**
   * Creates a Netty channel handler
   *
   * @param tag the name of the handler
   * @param group the channel group
   * @param listener the Netty event listener
   * @return a simple channel upstream handler
   */
  @Override
  public SimpleChannelUpstreamHandler createUpstreamHandler(
      final String tag, final ChannelGroup group, final NettyEventListener listener) {
    return new NettyChannelHandler(tag, group, listener);
  }
}
