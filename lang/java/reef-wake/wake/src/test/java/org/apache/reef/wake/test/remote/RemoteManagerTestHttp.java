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
package org.apache.reef.wake.test.remote;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.LoggingEventHandler;
import org.apache.reef.wake.impl.LoggingUtils;
import org.apache.reef.wake.impl.TimerStage;
import org.apache.reef.wake.remote.*;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.impl.DefaultRemoteIdentifierFactoryImplementation;
import org.apache.reef.wake.remote.impl.MultiCodec;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.remote.transport.TransportFactory;
import org.apache.reef.wake.remote.transport.netty.MessagingTransportFactory;
import org.apache.reef.wake.test.util.Monitor;
import org.apache.reef.wake.test.util.TimeoutHandler;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Tests for RemoteManagerFactory with HTTP Transport implementation.
 */
public class RemoteManagerTestHttp {

  private final LocalAddressProvider localAddressProvider;
  private final RemoteManagerFactory remoteManagerFactory;

  public RemoteManagerTestHttp() throws InjectionException {
    final JavaConfigurationBuilder builder = Tang.Factory.getTang().newConfigurationBuilder();
    builder.bindImplementation(TransportFactory.class, MessagingTransportFactory.class);
    builder.bindNamedParameter(RemoteConfiguration.Protocol.class, "101");
    final Injector injector = Tang.Factory.getTang().newInjector(builder.build());
    this.localAddressProvider = injector.getInstance(LocalAddressProvider.class);
    this.remoteManagerFactory = injector.getInstance(RemoteManagerFactory.class);
  }

  @Rule
  public final TestName name = new TestName();

  private static final String LOG_PREFIX = "TEST ";

  @Test
  public void testRemoteManagerTest() throws Exception {
    System.out.println(LOG_PREFIX + name.getMethodName());
    LoggingUtils.setLoggingLevel(Level.INFO);

    final Monitor monitor = new Monitor();
    final TimerStage timer = new TimerStage(new TimeoutHandler(monitor), 2000, 2000);

    final Map<Class<?>, Codec<?>> clazzToCodecMap = new HashMap<>();
    clazzToCodecMap.put(StartEvent.class, new ObjectSerializableCodec<StartEvent>());
    clazzToCodecMap.put(TestEvent.class, new ObjectSerializableCodec<TestEvent>());
    clazzToCodecMap.put(TestEvent1.class, new ObjectSerializableCodec<TestEvent1>());
    clazzToCodecMap.put(TestEvent2.class, new ObjectSerializableCodec<TestEvent2>());
    final Codec<?> codec = new MultiCodec<Object>(clazzToCodecMap);

    final String hostAddress = localAddressProvider.getLocalAddress();

    final RemoteManager rm = this.remoteManagerFactory.getInstance(
        "name", hostAddress, 0, codec, new LoggingEventHandler<Throwable>(), false, 3, 10000,
        localAddressProvider, Tang.Factory.getTang().newInjector().getInstance(TcpPortProvider.class));

    final RemoteIdentifier remoteId = rm.getMyIdentifier();
    Assert.assertTrue(rm.getMyIdentifier().equals(remoteId));

    final EventHandler<StartEvent> proxyConnection = rm.getHandler(remoteId, StartEvent.class);
    final EventHandler<TestEvent1> proxyHandler1 = rm.getHandler(remoteId, TestEvent1.class);
    final EventHandler<TestEvent2> proxyHandler2 = rm.getHandler(remoteId, TestEvent2.class);

    final AtomicInteger counter = new AtomicInteger(0);
    final int finalSize = 2;
    rm.registerHandler(StartEvent.class, new MessageTypeEventHandler<StartEvent>(rm, monitor, counter, finalSize));

    proxyConnection.onNext(new StartEvent());

    monitor.mwait();
    proxyHandler1.onNext(new TestEvent1("hello1", 0.0)); // registration after send expected to fail
    proxyHandler2.onNext(new TestEvent2("hello2", 1.0));

    monitor.mwait();

    Assert.assertEquals(finalSize, counter.get());

    rm.close();
    timer.close();
  }

  @Test
  public void testRemoteManagerConnectionRetryTest() throws Exception {
    final ExecutorService smExecutor = Executors.newFixedThreadPool(1);
    final ExecutorService rmExecutor = Executors.newFixedThreadPool(1);

    final RemoteManager sendingManager = getTestRemoteManager("sender", 9020, 3, 2000);

    final Future<Integer> smFuture = smExecutor.submit(new SendingRemoteManagerThread(sendingManager, 9010, 20000));
    Thread.sleep(1000);

    final RemoteManager receivingManager = getTestRemoteManager("receiver", 9010, 1, 2000);
    final Future<Integer> rmFuture = rmExecutor.submit(new ReceivingRemoteManagerThread(receivingManager, 20000, 1, 2));

    final int smCnt = smFuture.get();
    final int rmCnt = rmFuture.get();

    receivingManager.close();
    sendingManager.close();

    Assert.assertEquals(0, smCnt);
    Assert.assertEquals(2, rmCnt);
  }

  @Test
  public void testRemoteManagerConnectionRetryWithMultipleSenderTest() throws Exception {
    final int numOfSenderThreads = 5;
    final ExecutorService smExecutor = Executors.newFixedThreadPool(numOfSenderThreads);
    final ExecutorService rmExecutor = Executors.newFixedThreadPool(1);
    final ArrayList<Future<Integer>> smFutures = new ArrayList<>(numOfSenderThreads);

    final RemoteManager sendingManager = getTestRemoteManager("sender", 9030, 3, 5000);

    for (int i = 0; i < numOfSenderThreads; i++) {
      smFutures.add(smExecutor.submit(new SendingRemoteManagerThread(sendingManager, 9010, 20000)));
    }

    Thread.sleep(2000);

    final RemoteManager receivingManager = getTestRemoteManager("receiver", 9010, 1, 2000);
    final Future<Integer> receivingFuture =
        rmExecutor.submit(new ReceivingRemoteManagerThread(receivingManager, 20000, numOfSenderThreads, 2));

    // waiting sending remote manager.
    for (final Future<Integer> future : smFutures) {
      future.get();
    }

    // waiting receiving remote manager
    final int rmCnt = receivingFuture.get();

    sendingManager.close();
    receivingManager.close();

    // get the result
    Assert.assertEquals(2 * numOfSenderThreads, rmCnt);
  }

  @Test
  public void testRemoteManagerOrderingGuaranteeTest() throws Exception {
    System.out.println(LOG_PREFIX + name.getMethodName());
    LoggingUtils.setLoggingLevel(Level.INFO);

    final Monitor monitor = new Monitor();
    final TimerStage timer = new TimerStage(new TimeoutHandler(monitor), 2000, 2000);

    final Map<Class<?>, Codec<?>> clazzToCodecMap = new HashMap<>();
    clazzToCodecMap.put(StartEvent.class, new ObjectSerializableCodec<StartEvent>());
    clazzToCodecMap.put(TestEvent.class, new ObjectSerializableCodec<TestEvent>());
    clazzToCodecMap.put(TestEvent1.class, new ObjectSerializableCodec<TestEvent1>());
    clazzToCodecMap.put(TestEvent2.class, new ObjectSerializableCodec<TestEvent2>());
    final Codec<?> codec = new MultiCodec<Object>(clazzToCodecMap);

    final String hostAddress = localAddressProvider.getLocalAddress();

    final RemoteManager rm = this.remoteManagerFactory.getInstance(
        "name", hostAddress, 0, codec, new LoggingEventHandler<Throwable>(), true, 3, 10000,
        localAddressProvider, Tang.Factory.getTang().newInjector().getInstance(TcpPortProvider.class));

    final RemoteIdentifier remoteId = rm.getMyIdentifier();

    final EventHandler<StartEvent> proxyConnection = rm.getHandler(remoteId, StartEvent.class);
    final EventHandler<TestEvent1> proxyHandler1 = rm.getHandler(remoteId, TestEvent1.class);
    final EventHandler<TestEvent2> proxyHandler2 = rm.getHandler(remoteId, TestEvent2.class);

    final AtomicInteger counter = new AtomicInteger(0);
    final int finalSize = 2;
    rm.registerHandler(StartEvent.class, new MessageTypeEventHandler<StartEvent>(rm, monitor, counter, finalSize));

    proxyConnection.onNext(new StartEvent());

    monitor.mwait();

    proxyHandler1.onNext(new TestEvent1("hello1", 0.0));
    proxyHandler2.onNext(new TestEvent2("hello2", 1.0));

    monitor.mwait();

    Assert.assertEquals(finalSize, counter.get());

    rm.close();
    timer.close();
  }

  @Test
  public void testRemoteManagerPBufTest() throws Exception {
    System.out.println(LOG_PREFIX + name.getMethodName());
    LoggingUtils.setLoggingLevel(Level.INFO);

    final Monitor monitor = new Monitor();
    final TimerStage timer = new TimerStage(new TimeoutHandler(monitor), 2000, 2000);

    final Map<Class<?>, Codec<?>> clazzToCodecMap = new HashMap<>();
    clazzToCodecMap.put(TestEvent.class, new TestEventCodec());
    final Codec<?> codec = new MultiCodec<Object>(clazzToCodecMap);

    final String hostAddress = localAddressProvider.getLocalAddress();

    final RemoteManager rm = this.remoteManagerFactory.getInstance(
        "name", hostAddress, 0, codec, new LoggingEventHandler<Throwable>(), false, 3, 10000,
        localAddressProvider, Tang.Factory.getTang().newInjector().getInstance(TcpPortProvider.class));

    final RemoteIdentifier remoteId = rm.getMyIdentifier();

    final EventHandler<TestEvent> proxyHandler = rm.getHandler(remoteId, TestEvent.class);

    final AtomicInteger counter = new AtomicInteger(0);
    final int finalSize = 0;
    rm.registerHandler(TestEvent.class, new MessageTypeEventHandler<TestEvent>(rm, monitor, counter, finalSize));

    proxyHandler.onNext(new TestEvent("hello", 0.0));

    monitor.mwait();

    Assert.assertEquals(finalSize, counter.get());

    rm.close();
    timer.close();
  }

  @Test
  public void testRemoteManagerExceptionTest() {
    System.out.println(LOG_PREFIX + name.getMethodName());
    LoggingUtils.setLoggingLevel(Level.INFO);

    final Monitor monitor = new Monitor();
    final TimerStage timer = new TimerStage(new TimeoutHandler(monitor), 2000, 2000);

    final Map<Class<?>, Codec<?>> clazzToCodecMap = new HashMap<>();
    clazzToCodecMap.put(StartEvent.class, new ObjectSerializableCodec<StartEvent>());
    clazzToCodecMap.put(TestEvent.class, new ObjectSerializableCodec<TestEvent>());
    final Codec<?> codec = new MultiCodec<Object>(clazzToCodecMap);

    final ExceptionHandler errorHandler = new ExceptionHandler(monitor);

    try (final RemoteManager rm = remoteManagerFactory.getInstance("name", 0, codec, errorHandler)) {
      final RemoteIdentifier remoteId = rm.getMyIdentifier();

      final EventHandler<StartEvent> proxyConnection = rm.getHandler(remoteId, StartEvent.class);
      rm.registerHandler(StartEvent.class, new ExceptionGenEventHandler<StartEvent>("recvExceptionGen"));

      proxyConnection.onNext(new StartEvent());
      monitor.mwait();
      timer.close();

    } catch (final UnknownHostException e) {
      e.printStackTrace();
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  private RemoteManager getTestRemoteManager(final String rmName, final int localPort,
                                             final int retry, final int retryTimeout) {
    final Map<Class<?>, Codec<?>> clazzToCodecMap = new HashMap<>();
    clazzToCodecMap.put(StartEvent.class, new ObjectSerializableCodec<StartEvent>());
    clazzToCodecMap.put(TestEvent1.class, new ObjectSerializableCodec<TestEvent1>());
    clazzToCodecMap.put(TestEvent2.class, new ObjectSerializableCodec<TestEvent1>());
    final Codec<?> codec = new MultiCodec<Object>(clazzToCodecMap);

    final String hostAddress = localAddressProvider.getLocalAddress();
    try {
      TcpPortProvider tcpPortProvider = Tang.Factory.getTang().newInjector().getInstance(TcpPortProvider.class);
      return remoteManagerFactory.getInstance(rmName, hostAddress, localPort,
              codec, new LoggingEventHandler<Throwable>(), false, retry, retryTimeout,
              localAddressProvider, tcpPortProvider);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  private class SendingRemoteManagerThread implements Callable<Integer> {

    private final int remotePort;
    private final int timeout;
    private RemoteManager rm;

    SendingRemoteManagerThread(final RemoteManager rm, final int remotePort, final int timeout) {
      this.remotePort = remotePort;
      this.timeout = timeout;
      this.rm = rm;
    }

    @Override
    public Integer call() throws Exception {

      final Monitor monitor = new Monitor();
      final TimerStage timer = new TimerStage(new TimeoutHandler(monitor), timeout, timeout);

      final String hostAddress = localAddressProvider.getLocalAddress();
      final RemoteIdentifierFactory factory = new DefaultRemoteIdentifierFactoryImplementation();
      final RemoteIdentifier remoteId = factory.getNewInstance("socket://" + hostAddress + ":" + remotePort);

      final EventHandler<StartEvent> proxyConnection = rm.getHandler(remoteId, StartEvent.class);
      final EventHandler<TestEvent1> proxyHandler1 = rm.getHandler(remoteId, TestEvent1.class);
      final EventHandler<TestEvent2> proxyHandler2 = rm.getHandler(remoteId, TestEvent2.class);

      final AtomicInteger counter = new AtomicInteger(0);
      final int finalSize = 0;
      rm.registerHandler(StartEvent.class, new MessageTypeEventHandler<StartEvent>(rm, monitor, counter, finalSize));

      proxyConnection.onNext(new StartEvent());

      monitor.mwait();

      proxyHandler1.onNext(new TestEvent1("hello1", 0.0)); // registration after send expected to fail
      proxyHandler2.onNext(new TestEvent2("hello2", 0.0)); // registration after send expected to fail
      timer.close();

      return counter.get();
    }
  }

  private class ReceivingRemoteManagerThread implements Callable<Integer> {

    private final int timeout;
    private final int numOfConnection;
    private final int numOfEvent;
    private RemoteManager rm;

    ReceivingRemoteManagerThread(final RemoteManager rm, final int timeout,
                                 final int numOfConnection, final int numOfEvent) {
      this.rm = rm;
      this.timeout = timeout;
      this.numOfConnection = numOfConnection;
      this.numOfEvent = numOfEvent;
    }

    @Override
    public Integer call() throws Exception {

      final Monitor monitor = new Monitor();
      final TimerStage timer = new TimerStage(new TimeoutHandler(monitor), timeout, timeout);

      final AtomicInteger counter = new AtomicInteger(0);
      final int finalSize = numOfConnection * numOfEvent;
      rm.registerHandler(StartEvent.class, new MessageTypeEventHandler<StartEvent>(rm, monitor, counter, finalSize));

      for (int i = 0; i < numOfConnection; i++) {
        monitor.mwait();
      }
      monitor.mwait();
      timer.close();
      return counter.get();
    }
  }

  class MessageTypeEventHandler<T> implements EventHandler<RemoteMessage<T>> {

    private final RemoteManager rm;
    private final Monitor monitor;
    private final AtomicInteger counter;
    private final int finalSize;

    MessageTypeEventHandler(final RemoteManager rm, final Monitor monitor,
                            final AtomicInteger counter, final int finalSize) {
      this.rm = rm;
      this.monitor = monitor;
      this.counter = counter;
      this.finalSize = finalSize;
    }

    @Override
    public void onNext(final RemoteMessage<T> value) {

      final RemoteIdentifier id = value.getIdentifier();
      final T message = value.getMessage();

      System.out.println(this.getClass() + " " + value + " " + id.toString() + " " + message.toString());

      System.out.println("Sleeping to force a bug");
      // try {
      //   Thread.sleep(2000);
      //} catch (InterruptedException e) {

      //  e.printStackTrace();
      // }

      // register specific handlers
      rm.registerHandler(id, TestEvent1.class,
          new ConsoleEventHandler<TestEvent1>("console1", monitor, counter, finalSize));
      rm.registerHandler(id, TestEvent2.class,
          new ConsoleEventHandler<TestEvent2>("console2", monitor, counter, finalSize));
      monitor.mnotify();
    }
  }

  class ConsoleEventHandler<T> implements EventHandler<T> {

    private final String name;
    private final Monitor monitor;
    private final AtomicInteger counter;
    private final int finalSize;

    ConsoleEventHandler(final String name, final Monitor monitor, final AtomicInteger counter, final int finalSize) {
      this.name = name;
      this.monitor = monitor;
      this.counter = counter;
      this.finalSize = finalSize;
    }

    @Override
    public void onNext(final T value) {
      System.out.println(this.getClass() + " " + name + " " + value);
      if (counter.incrementAndGet() == finalSize) {
        System.out.println(this.getClass() + " notify counter: " + counter.get());
        monitor.mnotify();
      }
    }
  }

  class ExceptionGenEventHandler<T> implements EventHandler<RemoteMessage<T>> {

    private final String name;

    ExceptionGenEventHandler(final String name) {
      this.name = name;
    }

    @Override
    public void onNext(final RemoteMessage<T> value) {
      System.out.println(name + " " + value);
      throw new TestRuntimeException("Test exception");
    }
  }

  class ExceptionHandler implements EventHandler<Throwable> {

    private final Monitor monitor;

    ExceptionHandler(final Monitor monitor) {
      this.monitor = monitor;
    }

    @Override
    public void onNext(final Throwable value) {
      System.out.println("!!! ExceptionHandler called : " + value);
      monitor.mnotify();
    }
  }

  final class TestRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    TestRuntimeException(final String s) {
      super(s);
    }
  }
}
