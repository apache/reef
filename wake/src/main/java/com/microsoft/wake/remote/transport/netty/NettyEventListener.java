/**
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.wake.remote.transport.netty;

import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;

/**
 * Netty event listener
 */
interface NettyEventListener {
  
  /**
   * Handles the message event
   * @param e the message event
   */
  public void messageReceived(MessageEvent e);
  
  /**
   * Handles the exception event
   * @param e the exception event
   */
  public void exceptionCaught(ExceptionEvent e);
  
  /**
   * Handles the channel connected event
   * @param e the channel state event
   */
  public void channelConnected(ChannelStateEvent e);
  
  /**
   * Handles the channel closed event
   * @param e the channel state event
   */ 
  public void channelClosed(ChannelStateEvent e);
}
