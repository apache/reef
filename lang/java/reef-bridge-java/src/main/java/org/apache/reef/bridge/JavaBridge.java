/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.reef.bridge;

import org.apache.reef.bridge.message.Acknowledgement;
import org.apache.reef.bridge.message.Protocol;
import org.apache.reef.bridge.message.SystemOnStart;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.Date;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Implements the Avro message protocol between the Java and C# bridges.
 */
public final class JavaBridge extends MultiObserverImpl<JavaBridge> {
  private static final Logger LOG = Logger.getLogger(JavaBridge.class.getName());
  private static final int TIME_OUT = 5;
  private Network network;

  private final ArrayDeque<RemoteProcedureCall> rpcQueue = new ArrayDeque<>();
  private final HashMap<Long, RemoteProcedureCall> rpcMap = new HashMap<>();

  /**
   * Used to implement an RPC style interface which puts the caller
   * to sleep until the RPC completes.
   */
  private final class RemoteProcedureCall {
    private Lock lock;
    private Condition condition;

    /**
     * Allocates a reentrant lock and associated condition variable.
     */
    RemoteProcedureCall() {
      this.lock = new ReentrantLock();
      this.condition = lock.newCondition();
    }

    /**
     * Blocks the caller until signalWaitComplete() is called or a timeout occurs.
     * @return A boolean value that indicates whether or not a timeout occurred.
     */
    public boolean waitForSignal() {
      boolean timeOutOccurred = false;
      lock.lock();
      try {
        timeOutOccurred = !condition.await(TIME_OUT, SECONDS);
      } catch(Exception e) {
        LOG.log(Level.SEVERE, "Caught exception waiting for RPC to complete: " + e.getMessage());
      } finally {
        lock.unlock();
      }
      return timeOutOccurred;
    }

    /**
     * Wakes the thread sleeping in waitForSignal().
     */
    public void signalWaitComplete() {
      lock.lock();
      try {
        condition.signal();
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Caught exception signalling an RPC acknowledgement : " + e.getMessage());
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * Puts the caller to sleep on an acknowledgment message for the input message identifier.
   * @param messageIdentifier The message identifier of the acknowledgement message on which the caller is sleeping.
   */
  private void blockCaller(final long messageIdentifier) {
    RemoteProcedureCall call;
    synchronized (this) {
      // Get an RPC object to block the calling thread.
      if (rpcMap.containsKey(messageIdentifier)) {
        // This should never happen as message identifiers are unique.
        LOG.log(Level.SEVERE, "Duplicate message identifier in RPC map");
      }
      if (rpcQueue.isEmpty()) {
        rpcQueue.addLast(new RemoteProcedureCall());
      }
      call = rpcQueue.getFirst();
      rpcMap.put(messageIdentifier, call);
    }

    LOG.log(Level.INFO,
          "Putting the caller to sleep for message identifier [" + Long.toString(messageIdentifier) + "]");
    // Put the call to sleep until the ack comes back.
    boolean timeOut = call.waitForSignal();
    if (timeOut) {
      synchronized (this) {
        call = rpcMap.remove(messageIdentifier);
        rpcQueue.addLast(call);
      }
      LOG.log(Level.SEVERE, "RPC for message identifier [" + Long.toString(messageIdentifier) + "] timed out");
    }
  }

  /**
   * Wake the caller sleeping on the specified message identifier.
   * @param messageIdentifier The message identifier of the caller who should be released.
   */
  private void releaseCaller(final long messageIdentifier) {
    RemoteProcedureCall call;
    synchronized (this) {
      // Get the associated call object.
      call = rpcMap.remove(messageIdentifier);
      if (call == null) {
        LOG.log(Level.SEVERE,
            "Received acknowledgement for unknown message identifier: " + Long.toString(messageIdentifier));
      } else {
        // Signal the sleeper and recycle the call object.
        LOG.log(Level.INFO,
          "Waking the caller for message identifier [" + Long.toString(messageIdentifier) + "]");
        call.signalWaitComplete();
        rpcQueue.addLast(call);
      }
    }
  }

  /**
   * Implements the RPC interface to the C# side of the bridge.
   * @param localAddressProvider Used to find an available port on the local host.
   */
  public JavaBridge(final LocalAddressProvider localAddressProvider) {
    this.network = new Network(localAddressProvider, this);
    rpcQueue.push(new RemoteProcedureCall());
  }

  public InetSocketAddress getAddress() {
    return network.getAddress();
  }

  public void onError(final Exception error) {
    LOG.log(Level.SEVERE, "OnError: ", error.getMessage());
  }

  public void onCompleted() {
    LOG.log(Level.INFO, "OnCompleted");
  }

  /**
   * Processes protocol messages from the C# side of the bridge.
   * @param identifier A long value which is the unique message identifier.
   * @param protocol A reference to the received Avro protocol message.
   */
  public void onNext(final long identifier, final Protocol protocol) {
    LOG.log(Level.INFO, "+++++++Received protocol message: ["
        + Long.toString(identifier) + "] " + protocol.getOffset().toString());
  }

  /**
   * Releases the caller sleeping on the inpput acknowledgement message.
   * @param acknowledgement The incoming acknowledgement message whose call will be released.
   */
  public void onNext(final long identifier, final Acknowledgement acknowledgement) {
    LOG.log(Level.INFO, "Received acknowledgement message for id = [" + Long.toString(identifier) + "]");
    releaseCaller(acknowledgement.getMessageIdentifier());
  }

  public void callClrSystemOnStartHandler() {
    Date date = new Date();
    long messageIdentifier = network.send(new SystemOnStart(date.getTime()));
    blockCaller(messageIdentifier);
  }
}
