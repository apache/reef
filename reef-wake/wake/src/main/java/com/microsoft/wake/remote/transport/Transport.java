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
package com.microsoft.wake.remote.transport;

import java.io.IOException;
import java.net.SocketAddress;

import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Stage;
import com.microsoft.wake.remote.Encoder;

/**
 * Transport for sending and receiving data
 */
public interface Transport extends Stage {

  /**
   * Constructs with a listening port number, a client-side event handling stage, and a server-side event handling stage
   */
  
  /**
   * Returns a link for the remote address if cached; otherwise opens, caches and returns
   * When it opens a link for the remote address, only one attempt for the address is made at a given time
   * 
   * @param remoteAddr the remote socket address
   * @param encoder the encoder
   * @param listener the link listener
   * @return a link associated with the address
   * @throws IOException
   */  
  public <T> Link<T> open(SocketAddress remoteAddr, Encoder<? super T> encoder, LinkListener<? super T> listener) throws IOException;
  
  /**
   * Returns a link for the remote address if already cached; otherwise, returns null
   * @param remoteAddr the remote address
   * @return a link if already cached; otherwise, null
   */
  public <T> Link<T> get(SocketAddress remoteAddr);
  
  /**
   * Gets a server listening port of this transport
   * 
   * @return a listening port number
   */
  public int getListeningPort();
   
  /**
   * Gets a server local socket address of this transport
   * 
   * @return a server local socket address
   */
  public SocketAddress getLocalAddress();
  
  /**
   * Registers the exception handler
   * 
   * @param handler the exception handler
   */
  public void registerErrorHandler(EventHandler<Exception> handler);
}
