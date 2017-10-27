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
package org.apache.reef.bridge;

import org.apache.avro.specific.SpecificRecord;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.MultiObserver;
import org.apache.reef.wake.avro.ProtocolSerializer;
import org.apache.reef.wake.remote.RemoteMessage;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.impl.ByteCodec;
import org.apache.reef.wake.remote.impl.SocketRemoteIdentifier;
import org.apache.reef.wake.remote.RemoteIdentifier;
import org.apache.reef.wake.remote.RemoteManager;
import org.apache.reef.wake.remote.RemoteManagerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The CLR Bridge Network class aggregates a RemoteManager and
 * Protocol Serializer to provide a simple send/receive interface
 * between the Java and CLR sides of the bridge.
 */
public final class NetworkTransport {

  private static final Logger LOG = Logger.getLogger(NetworkTransport.class.getName());

  /** Remote manager to handle java-C# bridge communication. */
  private final RemoteManager remoteManager;

  private final ProtocolSerializer serializer;
  private final InetSocketAddress inetSocketAddress;
  private final InjectionFuture<MultiObserver> messageObserver;

  private EventHandler<byte[]> sender;

  /**
   * Sends and receives messages between the java bridge and C# bridge.
   * @param observer A multiobserver instance that will receive all incoming messages.
   */
  @Inject
  private NetworkTransport(
      final RemoteManagerFactory remoteManagerFactory,
      final LocalAddressProvider localAddressProvider,
      final ProtocolSerializer serializer,
      final InjectionFuture<MultiObserver> observer) {

    LOG.log(Level.FINE, "Java bridge network initializing");

    this.serializer = serializer;
    this.messageObserver = observer;

    this.remoteManager = remoteManagerFactory.getInstance(
        "JavaBridgeNetwork", localAddressProvider.getLocalAddress(), 0, new ByteCodec());

    // Get our address and port number.
    final RemoteIdentifier remoteIdentifier = this.remoteManager.getMyIdentifier();
    if (remoteIdentifier instanceof SocketRemoteIdentifier) {
      final SocketRemoteIdentifier socketIdentifier = (SocketRemoteIdentifier)remoteIdentifier;
      this.inetSocketAddress = socketIdentifier.getSocketAddress();
    } else {
      throw new RuntimeException("Identifier is not a SocketRemoteIdentifier: " + remoteIdentifier);
    }

    // Register as the message handler for any incoming messages.
    this.remoteManager.registerHandler(byte[].class, new LocalObserver());
  }

  /**
   * Sends a message to the C# side of the bridge.
   * @param message An Avro message class derived from SpecificRecord.
   * @throws RuntimeException if invoked before initialization is complete.
   */
  public void send(final long identifier, final SpecificRecord message) {
    if (sender != null) {
      sender.onNext(serializer.write(message, identifier));
    } else {
      final String msgClassName = message.getClass().getCanonicalName();
      LOG.log(Level.SEVERE, "Attempt to send message [{0}] before network is initialized", msgClassName);
      throw new RuntimeException("NetworkTransport not initialized: failed to send " + msgClassName);
    }
  }

  /**
   * Provides the IP address and port of the java bridge network.
   * @return A InetSockerAddress that contains the ip and port of the bridge network.
   */
  public InetSocketAddress getAddress() {
    return inetSocketAddress;
  }

  /**
   * Processes messages from the network remote manager.
   */
  private final class LocalObserver implements EventHandler<RemoteMessage<byte[]>> {

    /**
     * Deserialize and direct incoming messages to the registered MuiltiObserver event handler.
     * @param message A RemoteMessage<byte[]> object which will be deserialized.
     */
    @Override
    public void onNext(final RemoteMessage<byte[]> message) {
      LOG.log(Level.FINEST, "Received remote message: {0}", message);

      if (sender == null) {
        // Instantiate a network connection to the C# side of the bridge.
        // THERE COULD BE  A SECURITY ISSUE HERE WHERE SOMEONE SPOOFS THE
        // C# BRIDGE, WE RECEIVE IT FIRST, AND CONNECT TO THE SPOOFER,
        // THOUGH THE TIME WINDOW IS VERY SMALL.
        final RemoteIdentifier remoteIdentifier = message.getIdentifier();
        LOG.log(Level.FINE, "Connecting to: {0}", remoteIdentifier);
        sender = remoteManager.getHandler(remoteIdentifier, byte[].class);
      }

      // Deserialize the message and invoke the appropriate processing method.
      serializer.read(message.getMessage(), messageObserver.get());
    }
  }
}
