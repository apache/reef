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
package org.apache.reef.tang;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.ClassHierarchyException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.types.NamedObject;
import org.junit.Assert;
import org.junit.Test;

import javax.inject.Inject;
import java.util.*;

/**
 * A class contains tests for Tang NamedObject features.
 */
public class TestNamedObject {

  private final Tang tang = Tang.Factory.getTang();
  private final NamedObject dummyNo0 = tang.newNamedObject(EmptyClass.class, "dummyNo0");
  private final NamedObject dummyNo1 = tang.newNamedObject(EmptyClass.class, "dummyNo1");

  /**
   * Test code for binding different values to the same NamedParameters in separate NamedObjects.
   *
   * @throws InjectionException
   */
  @Test
  public void testNamedParameterInt() throws InjectionException {
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindNamedParameter(IntegerNP.class, "0", dummyNo0)
        .bindNamedParameter(IntegerNP.class, "1", dummyNo1);
    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
    final Integer int0 = injector.getNamedInstance(IntegerNP.class, dummyNo0);
    final Integer int1 = injector.getNamedInstance(IntegerNP.class, dummyNo1);

    Assert.assertEquals(int0, new Integer(0));
    Assert.assertEquals(int1, new Integer(1));
  }

  /**
   * Test code for binding different implementations to the same NamedParameters in separate NamedObjects.
   *
   * @throws InjectionException
   */
  @Test
  public void testNamedParameterImpl() throws InjectionException {
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindNamedParameter(SimpleNP.class, SimpleImpl.class, dummyNo0)
        .bindNamedParameter(SimpleNP.class, SimpleImplMinus.class, dummyNo1);
    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
    final SimpleIface s0 = injector.getNamedInstance(SimpleNP.class, dummyNo0);
    final SimpleIface s1 = injector.getNamedInstance(SimpleNP.class, dummyNo1);

    Assert.assertEquals(s0.getValue(), 1);
    Assert.assertEquals(s1.getValue(), -1);
  }

  /**
   * Test code for binding a implementation to a NamedObject.
   *
   * @throws InjectionException
   */
  @Test
  public void testNamedObjectBasic() throws InjectionException {
    final NamedObject<SimpleImpl> no0 = tang.newNamedObject(SimpleImpl.class, "no0");
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindNamedParameter(IntegerNP.class, "2", no0);
    final SimpleImpl s0 = Tang.Factory.getTang().newInjector(cb.build()).getNamedObjectInstance(no0);

    Assert.assertEquals(s0.getValue(), 2);
  }

  /**
   * Test code for binding a NamedObject to a interface.
   *
   * @throws InjectionException
   */
  @Test
  public void testBindNamedObject() throws InjectionException {
    final NamedObject no0 = tang.newNamedObject(SimpleImpl.class, "no0");
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder()
        .bindImplementation(SimpleIface.class, no0, null)
        .bindNamedParameter(IntegerNP.class, "2", no0);
    final SimpleIface s0 = Tang.Factory.getTang().newInjector(cb.build()).getInstance(SimpleIface.class);

    Assert.assertEquals(s0.getValue(), 2);
  }

  /**
   * Test code for separating a space of the NamedObject in a NamedObject.
   *
   * @throws InjectionException
   */
  @Test
  public void testNamedObjectInNamedObject() throws InjectionException {
    final NamedObject outerNo = tang.newNamedObject(WrapperImpl.class, "out");
    final NamedObject innerNo = tang.newNamedObject(SimpleImpl.class, "in");
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder()
        .bindNamedParameter(IntegerNP.class, "2", outerNo)
        .bindNamedParameter(IntegerNP.class, "3", innerNo)
        .bindNamedParameter(SimpleNP.class, innerNo, outerNo);
    final WrapperImpl w = Tang.Factory.getTang().newInjector(cb.build())
        .getNamedObjectInstance((NamedObject<WrapperImpl>)outerNo);

    Assert.assertEquals(w.getValue(), 2);
    Assert.assertEquals(w.getInnerValue(), 3);
  }

  @Test
  public void testVolatileInstance() throws InjectionException {
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
    final SimpleImpl si0 = new SimpleImpl(2);
    injector.bindVolatileInstance(SimpleImpl.class, si0, dummyNo0);
    final SimpleImpl si1 = injector.getInstance(SimpleImpl.class, dummyNo0);

    Assert.assertEquals(si1.getValue(), 2);
  }

  @Test
  public void testVolatileParameter() throws InjectionException {
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
    final SimpleImpl si0 = new SimpleImpl(2);
    injector.bindVolatileParameter(SimpleImplNP.class, si0, dummyNo0);
    final SimpleImpl si1 = injector.getNamedInstance(SimpleImplNP.class, dummyNo0);
    Assert.assertEquals(si1.getValue(), 2);
  }

  @Test
  public void testNamedObjectSetBinding() throws InjectionException {
    final NamedObject<SimpleImpl> setNo1 = tang.newNamedObject(SimpleImpl.class, "SetNo1");
    final NamedObject<SimpleImpl> setNo2 = tang.newNamedObject(SimpleImpl.class, "SetNo2");
    final NamedObject<SimpleImpl> setNo3 = tang.newNamedObject(SimpleImpl.class, "SetNo3");
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindNamedParameter(IntegerNP.class, "1", setNo1);
    cb.bindNamedParameter(IntegerNP.class, "2", setNo2);
    cb.bindNamedParameter(IntegerNP.class, "3", setNo3);
    cb.bindSetEntry(SimpleSetNP.class, setNo1);
    cb.bindSetEntry(SimpleSetNP.class, setNo2);
    cb.bindSetEntry(SimpleSetNP.class, setNo3);
    final Injector injector = tang.newInjector(cb.build());
    final SimpleSetImpl simpleSetImpl = injector.getInstance(SimpleSetImpl.class);
    final Set<SimpleImpl> expectedSet = new HashSet<>(Arrays.asList(new SimpleImpl(2), new SimpleImpl(1), new
        SimpleImpl(3)));
    Assert.assertEquals(simpleSetImpl.getSimpleSet(), expectedSet);
  }

  @Test
  public void testNamedObjectListBinding() throws InjectionException {
    final NamedObject<SimpleImpl> listNo1 = tang.newNamedObject(SimpleImpl.class, "ListNo1");
    final NamedObject<SimpleImpl> listNo2 = tang.newNamedObject(SimpleImpl.class, "ListNo2");
    final NamedObject<SimpleImpl> listNo3 = tang.newNamedObject(SimpleImpl.class, "ListNo3");
    final List<NamedObject<SimpleImpl>> boundList = Arrays.asList(listNo1, listNo2, listNo3);
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindList(SimpleListNP.class, boundList);
    cb.bindNamedParameter(IntegerNP.class, "2", listNo1);
    cb.bindNamedParameter(IntegerNP.class, "1", listNo2);
    cb.bindNamedParameter(IntegerNP.class, "3", listNo3);
    final Injector injector = tang.newInjector(cb.build());
    final SimpleListImpl simpleListImpl = injector.getInstance(SimpleListImpl.class);
    final List<SimpleImpl> expectedList = Arrays.asList(new SimpleImpl(2), new SimpleImpl(1), new SimpleImpl(3));
    Assert.assertEquals(expectedList, simpleListImpl.getSimpleList());
  }

  @Test
  public void testNamedObjectDAGBinding() throws InjectionException {
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    final NamedObject<NodeImpl> nodeA = tang.newNamedObject(NodeImpl.class, "A");
    final NamedObject<NodeImpl> nodeB = tang.newNamedObject(NodeImpl.class, "B");
    final NamedObject<NodeImpl> nodeC = tang.newNamedObject(NodeImpl.class, "C");
    final NamedObject<NodeImpl> nodeD = tang.newNamedObject(NodeImpl.class, "D");

    final String nodeNameA = "A";
    final String nodeNameB = "B";
    final String nodeNameC = "C";
    final String nodeNameD = "D";

    cb.bindNamedParameter(NodeName.class, nodeNameA, nodeA);
    cb.bindSetEntry(NextNodes.class, nodeB, nodeA);
    cb.bindSetEntry(NextNodes.class, nodeC, nodeA);

    cb.bindNamedParameter(NodeName.class, nodeNameB, nodeB);
    cb.bindSetEntry(NextNodes.class, nodeD, nodeB);

    cb.bindNamedParameter(NodeName.class, nodeNameC, nodeC);
    cb.bindSetEntry(NextNodes.class, nodeD, nodeC);

    cb.bindNamedParameter(NodeName.class, nodeNameD, nodeD);

    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
    final Node a = injector.getNamedObjectInstance(nodeA);
    Assert.assertEquals(a.getNodeName(), nodeNameA);
    Assert.assertEquals(a.getNextNodeNames(), new HashSet<>(Arrays.asList(nodeNameB, nodeNameC)));
    final Node b = injector.getNamedObjectInstance(nodeB);
    Assert.assertEquals(b.getNodeName(), nodeNameB);
    Assert.assertEquals(b.getNextNodeNames(), new HashSet<>(Arrays.asList(nodeNameD)));
    final Node c = injector.getNamedObjectInstance(nodeC);
    Assert.assertEquals(c.getNodeName(), nodeNameC);
    Assert.assertEquals(c.getNextNodeNames(), new HashSet<>(Arrays.asList(nodeNameD)));
    final Node d = injector.getNamedObjectInstance(nodeD);
    Assert.assertEquals(d.getNodeName(), nodeNameD);
    Assert.assertEquals(d.getNextNodeNames(), new HashSet<>());
  }

  @Test
  public void testNamedObjectCyclicInjectionInjectionFuture() throws InjectionException {
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    final NamedObject<WrapperImpl> wrapperANo = tang.newNamedObject(WrapperImpl.class, "WrapperA");
    final NamedObject<WrapperFuturistImpl> wrapperBNo = tang.newNamedObject(WrapperFuturistImpl.class,
        "WrapperB");
    final int wrapperAInteger = 1;
    final int wrapperBInteger = 2;

    cb.bindNamedParameter(IntegerNP.class, String.valueOf(wrapperAInteger), wrapperANo);
    cb.bindNamedParameter(SimpleNP.class, wrapperBNo, wrapperANo);
    cb.bindNamedParameter(IntegerNP.class, String.valueOf(wrapperBInteger), wrapperBNo);
    cb.bindImplementation(SimpleIface.class, wrapperANo, wrapperBNo);
    final Injector injector = tang.newInjector(cb.build());
    final WrapperImpl wrapperA = injector.getNamedObjectInstance(wrapperANo);
    final WrapperFuturistImpl wrapperB = injector.getNamedObjectInstance(wrapperBNo);
    Assert.assertEquals(wrapperA.getInnerValue(), wrapperBInteger);
    Assert.assertEquals(wrapperB.getInnerValue(), wrapperAInteger);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDuplicatedConfigurationInNamedObject() throws BindException {
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    final NamedObject<SimpleImpl> simpleNamedObject = tang.newNamedObject(SimpleImpl.class, "Simple");
    cb.bindNamedParameter(IntegerNP.class, "1", simpleNamedObject);
    cb.bindNamedParameter(IntegerNP.class, "2", simpleNamedObject);
    cb.build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDuplicatedNamedObjectName() throws BindException {
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    final NamedObject<SimpleImpl> simpleNamedObject = tang.newNamedObject(SimpleImpl.class, "A");
    final NamedObject<NodeImpl> nodeNamedObject = tang.newNamedObject(NodeImpl.class, "A");
    cb.bindNamedParameter(IntegerNP.class, "1", simpleNamedObject);
    cb.bindNamedParameter(NodeName.class, "A", nodeNamedObject);
    cb.build();
  }

  @Test(expected = BindException.class)
  public void testNamedObjectTypeMismatchInImplementation() throws BindException {
    final NamedObject simpleImplNO = tang.newNamedObject(NodeImpl.class, "SimpleImpl");
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindImplementation(SimpleIface.class, simpleImplNO);
  }

  @Test(expected = BindException.class)
  public void testNamedObjectTypeMismatchInNamedParameter() throws BindException {
    final NamedObject simpleImplNO = tang.newNamedObject(NodeImpl.class, "SimpleImpl");
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindNamedParameter(SimpleNP.class, simpleImplNO);
  }

  @Test(expected = BindException.class)
  public void testNamedObjectTypeMismatchInSet() throws BindException {
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    final NamedObject simpleNamedObject = tang.newNamedObject(NodeImpl.class, "SimpleImpl");
    cb.bindSetEntry(SimpleSetNP.class, simpleNamedObject);
    cb.build();
  }

  @Test(expected = BindException.class)
  public void testNamedObjectTypeMismatchInList() throws BindException {
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    final NamedObject simpleNamedObject = tang.newNamedObject(NodeImpl.class, "SimpleImpl");
    final List<NamedObject> boundList = Arrays.asList(simpleNamedObject);
    cb.bindList(SimpleListNP.class, boundList);
    cb.build();
  }

  @Test(expected = ClassHierarchyException.class)
  public void testNamedObjectCycleInjection() throws InjectionException {
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    final NamedObject<WrapperImpl> wrapperANo = tang.newNamedObject(WrapperImpl.class, "WrapperA");
    final NamedObject<WrapperImpl> wrapperBNo = tang.newNamedObject(WrapperImpl.class, "WrapperB");

    cb.bindNamedParameter(SimpleNP.class, wrapperBNo, wrapperANo);
    cb.bindNamedParameter(SimpleNP.class, wrapperANo, wrapperBNo);
    tang.newInjector(cb.build()).getNamedObjectInstance(wrapperANo);
  }

  @Test(expected = InjectionException.class)
  public void testNamedObjectMissingConfiguration() throws InjectionException {
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    final NamedObject<WrapperImpl> wrapperNo = tang.newNamedObject(WrapperImpl.class, "Wrapper");
    cb.bindNamedParameter(IntegerNP.class, "1", wrapperNo);
    tang.newInjector(cb.build()).getNamedObjectInstance(wrapperNo);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInterfaceTypedNamedObject() throws IllegalArgumentException {
    final NamedObject<SimpleIface> invalidNo = tang.newNamedObject(SimpleIface.class, "SimpleIface");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAbstractTypedNamedObject() throws IllegalArgumentException {
    final NamedObject<NodeAbstractImpl> invalidNo = tang.newNamedObject(NodeAbstractImpl.class, "NAI");
  }
}

class EmptyClass {
}

interface SimpleIface {
  int getValue();
}

class SimpleImpl implements SimpleIface {
  private final int value;

  @Inject
  SimpleImpl() {
    value = 1;
  }

  @Inject
  SimpleImpl(@Parameter(IntegerNP.class) final Integer i) {
    value = i;
  }

  public int getValue() {
    return value;
  }

  @Override
  public boolean equals(final Object o) {
    if (o.getClass() != this.getClass()) {
      return false;
    } else {
      SimpleImpl simpleImpl = (SimpleImpl) o;
      return this.getValue() == simpleImpl.getValue();
    }
  }

  @Override
  public int hashCode() {
    return new Integer(this.getValue()).hashCode();
  }
}

class SimpleImplMinus implements SimpleIface {
  private final int value;

  @Inject
  SimpleImplMinus() {
    value = -1;
  }

  @Inject
  SimpleImplMinus(@Parameter(IntegerNP.class) final Integer i) {
    value = -i;
  }

  @Override
  public int getValue() {
    return value;
  }
}

class WrapperImpl implements SimpleIface {
  private final int value;
  private final SimpleIface inner;

  @Inject
  WrapperImpl(@Parameter(IntegerNP.class) final Integer i, @Parameter(SimpleNP.class) final SimpleIface inner) {
    value = i;
    this.inner = inner;
  }

  @Override
  public int getValue() {
    return value;
  }

  public int getInnerValue() {
    return inner.getValue();
  }
}

class WrapperFuturistImpl implements SimpleIface {
  private final int value;
  private final InjectionFuture<SimpleIface> innerInjectionFuture;

  @Inject
  WrapperFuturistImpl(@Parameter(IntegerNP.class) final Integer i, final InjectionFuture<SimpleIface> inner) {
    this.value = i;
    this.innerInjectionFuture = inner;
  }

  @Override
  public int getValue() {
    return value;
  }

  public int getInnerValue() {
    return innerInjectionFuture.get().getValue();
  }
}

class SimpleListImpl {

  private final List<SimpleImpl> simpleList;

  @Inject
  SimpleListImpl(@Parameter(SimpleListNP.class) final List<SimpleImpl> simpleList) {
    this.simpleList = simpleList;
  }

  public List<SimpleImpl> getSimpleList() {
    return simpleList;
  }
}

class SimpleSetImpl {
  private final Set<SimpleImpl> simpleSet;

  @Inject
  SimpleSetImpl(@Parameter(SimpleSetNP.class) final Set<SimpleImpl> simpleSet) {
    this.simpleSet = simpleSet;
  }

  public Set<SimpleImpl> getSimpleSet() {
    return simpleSet;
  }
}

@NamedParameter()
class IntegerNP implements Name<Integer> {
}

@NamedParameter()
class SimpleNP implements Name<SimpleIface> {
}

@NamedParameter()
class SimpleImplNP implements Name<SimpleImpl> {
}

@NamedParameter()
class SimpleSetNP implements Name<Set<SimpleIface>> {
}

@NamedParameter()
class SimpleListNP implements Name<List<SimpleIface>> {
}

@NamedParameter()
class NodeName implements Name<String> {
}

@NamedParameter
class NextNodes implements Name<Set<Node>> {
}

interface Node {

  Set<String> getNextNodeNames();

  String getNodeName();
}

abstract class NodeAbstractImpl implements Node {
  @Override
  public abstract Set<String> getNextNodeNames();

  @Override
  public abstract String getNodeName();
}

class NodeImpl implements Node {

  private final Set<String> nextNodeNames;
  private final String nodeName;

  @Inject
  NodeImpl(@Parameter(NextNodes.class) final Set<Node> nextNodes,
           @Parameter(NodeName.class) final String nodeName) {
    this.nextNodeNames = new HashSet<>();
    for(final Node nextNode: nextNodes) {
      this.nextNodeNames.add(nextNode.getNodeName());
    }
    this.nodeName = nodeName;
  }

  @Override
  public Set<String> getNextNodeNames() {
    return nextNodeNames;
  }

  @Override
  public String getNodeName() {
    return nodeName;
  }
}
