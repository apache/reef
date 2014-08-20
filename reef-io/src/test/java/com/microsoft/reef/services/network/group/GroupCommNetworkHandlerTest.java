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
package com.microsoft.reef.services.network.group;

import com.microsoft.reef.io.network.Message;
import com.microsoft.reef.io.network.group.impl.GroupCommNetworkHandler;
import com.microsoft.reef.io.network.group.impl.Handler;
import com.microsoft.reef.io.network.impl.NSMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.reef.io.network.util.Utils;
import com.microsoft.reef.services.network.util.TestUtils;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.ComparableIdentifier;
import com.microsoft.wake.EStage;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.impl.SingleThreadStage;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GroupCommNetworkHandlerTest {


  private static final Logger LOG = Logger.getLogger(GroupCommNetworkHandlerTest.class.getName());
  static StringIdentifierFactory idFac = new StringIdentifierFactory();
  static ComparableIdentifier id1, id2;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    id1 = (ComparableIdentifier) idFac.getNewInstance("Task1");
    id2 = (ComparableIdentifier) idFac.getNewInstance("Task2");
  }

  /**
   * Test method for {@link com.microsoft.reef.io.network.group.impl.GroupCommNetworkHandler#GroupCommNetworkHandler via TANG}.
   *
   * @throws BindException
   * @throws InjectionException
   */
  @Test(timeout = 2000)
  public final void testGroupCommNetworkHandlerTang() throws BindException, InjectionException {
    List<ComparableIdentifier> idLst = new ArrayList<>();
    idLst.add(id1);
    idLst.add(id2);
    String ids = Utils.listToString(idLst);
    Tang tang = Tang.Factory.getTang();
    JavaConfigurationBuilder jcb = tang.newConfigurationBuilder();
    jcb.bindNamedParameter(GroupCommNetworkHandler.IDs.class, ids);
    GroupCommNetworkHandler gcnhTang = tang.newInjector(jcb.build()).getInstance(GroupCommNetworkHandler.class);
    Assert.assertNotNull("tang.getInstance(GroupCommNetworkHandler.class)", gcnhTang);
  }

  /**
   * Test method for {@link com.microsoft.reef.io.network.group.impl.GroupCommNetworkHandler#GroupCommNetworkHandler(java.lang.String, com.microsoft.wake.IdentifierFactory, int)}.
   *
   * @throws BindException
   * @throws InjectionException
   */
  @Test(timeout = 2000)
  public final void testGroupCommNetworkHandlerStringIdentifierFactoryInt() throws BindException, InjectionException {
    List<ComparableIdentifier> idLst = new ArrayList<>();
    idLst.add(id1);
    idLst.add(id2);
    String ids = Utils.listToString(idLst);
    GroupCommNetworkHandler gcnh = new GroupCommNetworkHandler(ids, idFac, 5);
    Assert.assertNotNull("new GCNH(String, IdentifierFactory, int)", gcnh);
  }

  /**
   * Test method for {@link com.microsoft.reef.io.network.group.impl.GroupCommNetworkHandler#GroupCommNetworkHandler(java.util.List, com.microsoft.wake.IdentifierFactory, int)}.
   */
  @Test(timeout = 2000)
  public final void testGroupCommNetworkHandlerListOfIdentifierIdentifierFactoryInt() {
    List<Identifier> ids = new ArrayList<>();
    ids.add(id1);
    ids.add(id2);
    GroupCommNetworkHandler gcnh = new GroupCommNetworkHandler(ids, idFac, 5);
    Assert.assertNotNull("new GCNH(List<Identifier>, IdentifierFactory, int)", gcnh);
  }

  /**
   * Test method for {@link com.microsoft.reef.io.network.group.impl.GroupCommNetworkHandler#getHandler(com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type)}.
   */
  @Test(timeout = 2000)
  public final void testGetHandler() {
    List<Identifier> ids = new ArrayList<>();
    ids.add(id1);
    ids.add(id2);
    GroupCommNetworkHandler gcnh = new GroupCommNetworkHandler(ids, idFac, 5);
    for (GroupCommMessage.Type type : GroupCommMessage.Type.values()) {
      if (TestUtils.controlMessage(type)) continue;
      Handler h = gcnh.getHandler(type);
      Assert.assertNotNull("GCNH.getHandler( " + type + " )", h);
    }
  }

  /**
   * Test method for {@link com.microsoft.reef.io.network.group.impl.GroupCommNetworkHandler#onNext(com.microsoft.reef.io.network.Message)}.
   *
   * @throws Exception
   */
  @Test(timeout = 5000)
  public final void testOnNext() throws Exception {
    List<Identifier> ids = new ArrayList<>();
    ids.add(id1);
    ids.add(id2);
    for (int capacity = 1; capacity <= 100; capacity++) {
      LOG.log(Level.FINEST, "*************Capacity = " + capacity + "*****************");
      GroupCommNetworkHandler gcnh = new GroupCommNetworkHandler(ids, idFac, capacity);
      int[] msgsPerType = new int[GroupCommMessage.Type.values().length];
      Random r = new Random(1331);
      int totCapacity = 0;
      for (int i = 0; i < msgsPerType.length; i++) {
        int cap = r.nextInt(capacity);
        msgsPerType[i] = cap;
        totCapacity += cap;
      }
      //totCapacity should be at least 1
      totCapacity = Math.max(totCapacity, 1);
      LOG.log(Level.FINE, Arrays.toString(msgsPerType));
      LOG.log(Level.FINE, Integer.toString(totCapacity));
      try (EStage<Message<GroupCommMessage>> stage = new SingleThreadStage<>(
          gcnh, totCapacity)) {
        for (Type type : GroupCommMessage.Type.values()) {
          if (TestUtils.controlMessage(type)) continue;
          for (int i = 0; i < msgsPerType[type.ordinal()]; i++) {
            String msgStr = "Hello" + type.toString() + i;
            LOG.log(Level.FINE, "Message: " + msgStr);
            GroupCommMessage exp = TestUtils.bldGCM(type, id1, id2, msgStr.getBytes());
            Message<GroupCommMessage> m = new NSMessage<ReefNetworkGroupCommProtos.GroupCommMessage>(
                id1, id2, exp);
            stage.onNext(m);
          }
        }
        for (Type type : GroupCommMessage.Type.values()) {
          if (TestUtils.controlMessage(type)) continue;
          for (int i = 0; i < msgsPerType[type.ordinal()]; i++) {
            String msgStr = "Hello" + type.toString() + i;
            GroupCommMessage ret = gcnh.getHandler(type).getData(id1);
            Assert.assertEquals("Source ID:", id1.toString(), ret.getSrcid());
            Assert.assertEquals("Dest ID:", id2.toString(), ret.getDestid());
            Assert.assertArrayEquals("Message:", msgStr.getBytes(), ret
                .getMsgs(0).getData().toByteArray());
          }
        }
      }
    }
  }
}
