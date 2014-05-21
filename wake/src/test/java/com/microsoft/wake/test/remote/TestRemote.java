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
package com.microsoft.wake.test.remote;

import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.LoggingEventHandler;
import com.microsoft.wake.remote.*;
import com.microsoft.wake.remote.impl.DefaultRemoteIdentifierFactoryImplementation;
import com.microsoft.wake.remote.impl.DefaultRemoteManagerImplementation;

import java.net.UnknownHostException;

public class TestRemote {

  public static void main(String[] args) {
    String hostAddress = NetUtils.getLocalAddress();
    int myPort = 10011;
    int remotePort = 10001;
    Codec<TestEvent> codec = new TestEventCodec();
    try (RemoteManager rm = new DefaultRemoteManagerImplementation("name", hostAddress,
        myPort, codec, new LoggingEventHandler<Throwable>(), false, 1, 10000)) {
      // proxy handler
      RemoteIdentifierFactory factory = new DefaultRemoteIdentifierFactoryImplementation();
      RemoteIdentifier remoteId = factory.getNewInstance("socket://" + hostAddress + ":" + remotePort);
      EventHandler<TestEvent> proxyHandler = rm.getHandler(remoteId, TestEvent.class);

      proxyHandler.onNext(new TestEvent("hello", 1.0));
      // register a handler
      rm.registerHandler(TestEvent.class, new TestEventHandler(proxyHandler));

    } catch (UnknownHostException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

class TestEventHandler implements EventHandler<RemoteMessage<TestEvent>> {

  private final EventHandler<TestEvent> proxy;

  public TestEventHandler(EventHandler<TestEvent> proxy) {
    this.proxy = proxy;
  }

  @Override
  public void onNext(RemoteMessage<TestEvent> value) {
    System.out.println(value.getMessage().getMessage() + " " + value.getMessage().getLoad());
    proxy.onNext(value.getMessage());
  }
}
