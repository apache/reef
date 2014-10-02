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
package com.microsoft.tang.implementation.protobuf;

import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.proto.ClassHierarchyProto;
import com.microsoft.tang.types.Node;
import org.junit.Assert;
import org.junit.Test;

import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.implementation.TangImpl;
import com.microsoft.tang.implementation.TestClassHierarchy;

import java.io.*;

public class TestClassHierarchyRoundTrip extends TestClassHierarchy{

  private void setup1() {
    TangImpl.reset();
    ns = Tang.Factory.getTang().getDefaultClassHierarchy();
  }

  private void setup2() {
    TangImpl.reset();
    ns = new ProtocolBufferClassHierarchy(ProtocolBufferClassHierarchy.serialize(ns));
  }

  private void setup3() {
    TangImpl.reset();
    try {
      final File file = java.io.File.createTempFile("testProto", "bin");
      ProtocolBufferClassHierarchy.serialize(file, ns);
      ns = ProtocolBufferClassHierarchy.deserialize(file);
      file.delete();
    } catch (IOException e ) {
      Assert.fail(String.format("IOException when serialize/deserialize proto buffer file ", e));
    }
  }

  @Test
  @Override
  public void testJavaString() throws NameResolutionException {
    setup1();
    super.testJavaString();
    setup2();
    super.testJavaString();
    setup3();
    super.testJavaString();
  }
  
  @Test
  @Override
  public void testSimpleConstructors() throws NameResolutionException {
    setup1();
    super.testSimpleConstructors();
    setup2();
    super.testSimpleConstructors();
    setup3();
    super.testSimpleConstructors();
  }

  @Test
  @Override
  public void testNamedParameterConstructors() throws NameResolutionException {
    setup1();
    super.testNamedParameterConstructors();
    setup2();
    super.testNamedParameterConstructors();
    setup3();
    super.testNamedParameterConstructors();
  }

  @Test
  @Override
  public void testArray() throws NameResolutionException {
    setup1();
    super.testArray();
    setup2();
    super.testArray();
    setup3();
    super.testArray();
  }

  @Test
  @Override
  public void testRepeatConstructorArg() throws NameResolutionException {
    setup1();
    super.testRepeatConstructorArg();
    setup2();
    super.testRepeatConstructorArg();
    setup3();
    super.testRepeatConstructorArg();
  }

  @Test
  @Override
  public void testRepeatConstructorArgClasses() throws NameResolutionException {
    setup1();
    super.testRepeatConstructorArgClasses();
    setup2();
    super.testRepeatConstructorArgClasses();
    setup3();
    super.testRepeatConstructorArgClasses();
  }

  @Test
  @Override
  public void testLeafRepeatedConstructorArgClasses() throws NameResolutionException {
    setup1();
    super.testLeafRepeatedConstructorArgClasses();
    setup2();
    super.testLeafRepeatedConstructorArgClasses();
    setup3();
    super.testLeafRepeatedConstructorArgClasses();
  }

  @Test
  @Override
  public void testNamedRepeatConstructorArgClasses() throws NameResolutionException {
    setup1();
    super.testNamedRepeatConstructorArgClasses();
    setup2();
    super.testNamedRepeatConstructorArgClasses();
    setup3();
    super.testNamedRepeatConstructorArgClasses();
  }

  @Test
  @Override
  public void testResolveDependencies() throws NameResolutionException {
    setup1();
    super.testResolveDependencies();
    setup2();
    super.testResolveDependencies();
    setup3();
    super.testResolveDependencies();
  }

  @Test
  @Override
  public void testDocumentedLocalNamedParameter() throws NameResolutionException {
    setup1();
    super.testDocumentedLocalNamedParameter();
    setup2();
    super.testDocumentedLocalNamedParameter();
    setup3();
    super.testDocumentedLocalNamedParameter();
  }

  @Test
  @Override
  public void testNamedParameterTypeMismatch() throws NameResolutionException {
    setup1();
    super.testNamedParameterTypeMismatch();
    setup2();
    super.testNamedParameterTypeMismatch();
    setup3();
    super.testNamedParameterTypeMismatch();
  }

  @Test
  @Override
  public void testUnannotatedName() throws NameResolutionException {
    setup1();
    super.testUnannotatedName();
    setup2();
    super.testUnannotatedName();
    setup3();
    super.testUnannotatedName();
  }

  @Test
  @Override
  public void testAnnotatedNotName() throws NameResolutionException {
    setup1();
    super.testAnnotatedNotName();
    setup2();
    super.testAnnotatedNotName();
    setup3();
    super.testAnnotatedNotName();
  }

  @Test
  @Override
  public void testGenericTorture1() throws NameResolutionException {
    setup1();
    super.testGenericTorture1();
    setup2();
    super.testGenericTorture1();
    setup3();
    super.testGenericTorture1();
  }

  @Test
  @Override
  public void testGenericTorture2() throws NameResolutionException {
    setup1();
    super.testGenericTorture2();
    setup2();
    super.testGenericTorture2();
    setup3();
    super.testGenericTorture2();
  }

  @Test
  @Override
  public void testGenericTorture3() throws NameResolutionException {
    setup1();
    super.testGenericTorture3();
    setup2();
    super.testGenericTorture3();
    setup3();
    super.testGenericTorture3();
  }

  @Test
  @Override
  public void testGenericTorture4() throws NameResolutionException {
    setup1();
    super.testGenericTorture4();
    setup2();
    super.testGenericTorture4();
    setup3();
    super.testGenericTorture4();
  }

  @Test
  @Override
  public void testGenericTorture5() throws NameResolutionException {
    setup1();
    super.testGenericTorture5();
    setup2();
    super.testGenericTorture5();
    setup3();
    super.testGenericTorture5();
  }

  @Test
  @Override
  public void testGenericTorture6() throws NameResolutionException {
    setup1();
    super.testGenericTorture6();
    setup2();
    super.testGenericTorture6();
    setup3();
    super.testGenericTorture6();
  }

  @Test
  @Override
  public void testGenericTorture7() throws NameResolutionException {
    setup1();
    super.testGenericTorture7();
    setup2();
    super.testGenericTorture7();
    setup3();
    super.testGenericTorture7();
  }

  @Test
  @Override
  public void testGenericTorture8() throws NameResolutionException {
    setup1();
    super.testGenericTorture8();
    setup2();
    super.testGenericTorture8();
    setup3();
    super.testGenericTorture8();
  }

  @Test
  @Override
  public void testGenericTorture9() throws NameResolutionException {
    setup1();
    super.testGenericTorture9();
    setup2();
    super.testGenericTorture9();
    setup3();
    super.testGenericTorture9();
  }

  @Test
  @Override
  public void testGenericTorture10() throws NameResolutionException {
    setup1();
    super.testGenericTorture10();
    setup2();
    super.testGenericTorture10();
  }
  @Test
  @Override
  public void testGenericTorture11() throws NameResolutionException {
    setup1();
    super.testGenericTorture11();
    setup2();
    super.testGenericTorture11();
  }
  @Test
  @Override
  public void testGenericTorture12() throws NameResolutionException {
    setup1();
    super.testGenericTorture12();
    setup2();
    super.testGenericTorture12();
  }
  @Test
  @Override
  public void testInjectNonStaticLocalArgClass() throws NameResolutionException {
    setup1();
    super.testInjectNonStaticLocalArgClass();
    setup2();
    super.testInjectNonStaticLocalArgClass();
    setup3();
    super.testInjectNonStaticLocalArgClass();
  }

  @Test
  @Override
  public void testOKShortNames() throws NameResolutionException {
    setup1();
    super.testOKShortNames();
    setup2();
    super.testOKShortNames();
    setup3();
    super.testOKShortNames();
  }

  @Test
  @Override
  public void testRoundTripInnerClassNames() throws NameResolutionException, ClassNotFoundException {
    setup1();
    super.testRoundTripInnerClassNames();
    setup2();
    super.testRoundTripInnerClassNames();
    setup3();
    super.testRoundTripInnerClassNames();
  }

  @Test
  @Override
  public void testUnitIsInjectable() throws NameResolutionException, InjectionException {
    setup1();
    super.testUnitIsInjectable();
    setup2();
    super.testUnitIsInjectable();
    setup3();
    super.testUnitIsInjectable();
  }

  @Test
  @Override
  public void testBadUnitDecl() throws NameResolutionException {
    setup1();
    super.testBadUnitDecl();
    setup2();
    super.testBadUnitDecl();
    setup3();
    super.testBadUnitDecl();
  }

  @Test
  @Override
  public void nameCantBindWrongSubclassAsDefault() throws NameResolutionException {
    setup1();
    super.nameCantBindWrongSubclassAsDefault();
    setup2();
    super.nameCantBindWrongSubclassAsDefault();
    setup3();
    super.nameCantBindWrongSubclassAsDefault();
  }

  @Test
  @Override
  public void ifaceCantBindWrongImplAsDefault() throws NameResolutionException {
    setup1();
    super.ifaceCantBindWrongImplAsDefault();
    setup2();
    super.ifaceCantBindWrongImplAsDefault();
    setup3();
    super.ifaceCantBindWrongImplAsDefault();
  }
}
