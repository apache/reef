package com.microsoft.wake.remote.transport.netty;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;

/**
 * Default Netty channel handler factory
 */
final class NettyDefaultChannelHandlerFactory implements NettyChannelHandlerFactory {
  
  private final String tag;
  private final ChannelGroup group;
  private final NettyEventListener listener;

  NettyDefaultChannelHandlerFactory(String tag, ChannelGroup group, NettyEventListener listener) {
    this.tag = tag;
    this.group = group;
    this.listener = listener;
  }
  
  /**
   * Creates a Netty channel handler
   *
   * @param tag the name of the handler
   * @param group the channel group
   * @param listener the Netty event listener
   * @return a simple channel upstream handler
   */
  @Override
  public ChannelInboundHandlerAdapter createChannelInboundHandler() {
    return new NettyChannelHandler(this.tag, this.group, this.listener);
  }
}
