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
package org.apache.reef.tang.implementation.avro;

import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.exceptions.NameResolutionException;
import org.apache.reef.tang.implementation.TangImpl;
import org.apache.reef.tang.implementation.TestClassHierarchy;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestClassHierarchyRoundTrip extends TestClassHierarchy {
  // The default ClassHierarchy
  private void setup0() {
    TangImpl.reset();
    ns = Tang.Factory.getTang().getDefaultClassHierarchy();
  }

  // Serializes ClassHierarchy to file
  private void setup1() {
    TangImpl.reset();
    try {
      final File file = java.io.File.createTempFile("TangTest", "avroch");
      serializer.toFile(ns, file);
      ns = serializer.fromFile(file);
      file.delete();
    } catch (final IOException e) {
      e.printStackTrace();
      Assert.fail("IOException when serialize/deserialize the ClassHierarchy");
    }
  }

  // Serializes ClassHierarchy to TextFile
  private void setup2() {
    TangImpl.reset();
    try {
      final File textFile = java.io.File.createTempFile("TangTest", "avroch");
      serializer.toTextFile(ns, textFile);
      ns = serializer.fromTextFile(textFile);
      textFile.delete();
    } catch (final IOException e) {
      e.printStackTrace();
      Assert.fail("IOException when serialize/deserialize the ClassHierarchy");
    }
  }

  // Serializes ClassHierarchy to byte[]
  private void setup3() {
    TangImpl.reset();
    try {
      ns = serializer.fromByteArray(serializer.toByteArray(ns));
    } catch (final IOException e) {
      e.printStackTrace();
      Assert.fail("IOException when serialize/deserialize the ClassHierarchy");
    }
  }

  // Serializes ClassHierarchy to String
  private void setup4() {
    TangImpl.reset();
    try {
      ns = serializer.fromString(serializer.toString(ns));
    } catch (final IOException e) {
      e.printStackTrace();
      Assert.fail("IOException when serialize/deserialize the ClassHierarchy");
    }
  }

  @Test
  @Override
  public void testJavaString() throws NameResolutionException {
    setup0();
    super.testJavaString();
    setup1();
    super.testJavaString();
    setup2();
    super.testJavaString();
    setup3();
    super.testJavaString();
    setup4();
    super.testJavaString();
  }

  @Test
  @Override
  public void testSimpleConstructors() throws NameResolutionException {
    setup0();
    super.testSimpleConstructors();
    setup1();
    super.testSimpleConstructors();
    setup2();
    super.testSimpleConstructors();
    setup3();
    super.testSimpleConstructors();
    setup4();
    super.testSimpleConstructors();
  }

  @Test
  @Override
  public void testNamedParameterConstructors() throws NameResolutionException {
    setup0();
    super.testNamedParameterConstructors();
    setup1();
    super.testNamedParameterConstructors();
    setup2();
    super.testNamedParameterConstructors();
    setup3();
    super.testNamedParameterConstructors();
    setup4();
    super.testNamedParameterConstructors();
  }

  @Test
  @Override
  public void testArray() throws NameResolutionException {
    setup0();
    super.testArray();
    setup1();
    super.testArray();
    setup2();
    super.testArray();
    setup3();
    super.testArray();
    setup4();
    super.testArray();
  }

  @Test
  @Override
  public void testRepeatConstructorArg() throws NameResolutionException {
    setup0();
    super.testRepeatConstructorArg();
    setup1();
    super.testRepeatConstructorArg();
    setup2();
    super.testRepeatConstructorArg();
    setup3();
    super.testRepeatConstructorArg();
    setup4();
    super.testRepeatConstructorArg();
  }

  @Test
  @Override
  public void testRepeatConstructorArgClasses() throws NameResolutionException {
    setup0();
    super.testRepeatConstructorArgClasses();
    setup1();
    super.testRepeatConstructorArgClasses();
    setup2();
    super.testRepeatConstructorArgClasses();
    setup3();
    super.testRepeatConstructorArgClasses();
    setup4();
    super.testRepeatConstructorArgClasses();
  }

  @Test
  @Override
  public void testLeafRepeatedConstructorArgClasses() throws NameResolutionException {
    setup0();
    super.testLeafRepeatedConstructorArgClasses();
    setup1();
    super.testLeafRepeatedConstructorArgClasses();
    setup2();
    super.testLeafRepeatedConstructorArgClasses();
    setup3();
    super.testLeafRepeatedConstructorArgClasses();
    setup4();
    super.testLeafRepeatedConstructorArgClasses();
  }

  @Test
  @Override
  public void testNamedRepeatConstructorArgClasses() throws NameResolutionException {
    setup0();
    super.testNamedRepeatConstructorArgClasses();
    setup1();
    super.testNamedRepeatConstructorArgClasses();
    setup2();
    super.testNamedRepeatConstructorArgClasses();
    setup3();
    super.testNamedRepeatConstructorArgClasses();
    setup4();
    super.testNamedRepeatConstructorArgClasses();
  }

  @Test
  @Override
  public void testResolveDependencies() throws NameResolutionException {
    setup0();
    super.testResolveDependencies();
    setup1();
    super.testResolveDependencies();
    setup2();
    super.testResolveDependencies();
    setup3();
    super.testResolveDependencies();
    setup4();
    super.testResolveDependencies();
  }

  @Test
  @Override
  public void testDocumentedLocalNamedParameter() throws NameResolutionException {
    setup0();
    super.testDocumentedLocalNamedParameter();
    setup1();
    super.testDocumentedLocalNamedParameter();
    setup2();
    super.testDocumentedLocalNamedParameter();
    setup3();
    super.testDocumentedLocalNamedParameter();
    setup4();
    super.testDocumentedLocalNamedParameter();
  }

  @Test
  @Override
  public void testNamedParameterTypeMismatch() throws NameResolutionException {
    setup0();
    super.testNamedParameterTypeMismatch();
    setup1();
    super.testNamedParameterTypeMismatch();
    setup2();
    super.testNamedParameterTypeMismatch();
    setup3();
    super.testNamedParameterTypeMismatch();
    setup4();
    super.testNamedParameterTypeMismatch();
  }

  @Test
  @Override
  public void testUnannotatedName() throws NameResolutionException {
    setup0();
    super.testUnannotatedName();
    setup1();
    super.testUnannotatedName();
    setup2();
    super.testUnannotatedName();
    setup3();
    super.testUnannotatedName();
    setup4();
    super.testUnannotatedName();
  }

  @Test
  @Override
  public void testAnnotatedNotName() throws NameResolutionException {
    setup0();
    super.testAnnotatedNotName();
    setup1();
    super.testAnnotatedNotName();
    setup2();
    super.testAnnotatedNotName();
    setup3();
    super.testAnnotatedNotName();
    setup4();
    super.testAnnotatedNotName();
  }

  @Test
  @Override
  public void testGenericTorture1() throws NameResolutionException {
    setup0();
    super.testGenericTorture1();
    setup1();
    super.testGenericTorture1();
    setup2();
    super.testGenericTorture1();
    setup3();
    super.testGenericTorture1();
    setup4();
    super.testGenericTorture1();
  }

  @Test
  @Override
  public void testGenericTorture3() throws NameResolutionException {
    setup0();
    super.testGenericTorture3();
    setup1();
    super.testGenericTorture3();
    setup2();
    super.testGenericTorture3();
    setup3();
    super.testGenericTorture3();
    setup4();
    super.testGenericTorture3();
  }

  @Test
  @Override
  public void testGenericTorture4() throws NameResolutionException {
    setup0();
    super.testGenericTorture4();
    setup1();
    super.testGenericTorture4();
    setup2();
    super.testGenericTorture4();
    setup3();
    super.testGenericTorture4();
    setup4();
    super.testGenericTorture4();
  }

  @Test
  @Override
  public void testGenericTorture5() throws NameResolutionException {
    setup0();
    super.testGenericTorture5();
    setup1();
    super.testGenericTorture5();
    setup2();
    super.testGenericTorture5();
    setup3();
    super.testGenericTorture5();
    setup4();
    super.testGenericTorture5();
  }

  @Test
  @Override
  public void testGenericTorture6() throws NameResolutionException {
    setup0();
    super.testGenericTorture6();
    setup1();
    super.testGenericTorture6();
    setup2();
    super.testGenericTorture6();
    setup3();
    super.testGenericTorture6();
    setup4();
    super.testGenericTorture6();
  }

  @Test
  @Override
  public void testGenericTorture7() throws NameResolutionException {
    setup0();
    super.testGenericTorture7();
    setup1();
    super.testGenericTorture7();
    setup2();
    super.testGenericTorture7();
    setup3();
    super.testGenericTorture7();
    setup4();
    super.testGenericTorture7();
  }

  @Test
  @Override
  public void testGenericTorture8() throws NameResolutionException {
    setup0();
    super.testGenericTorture8();
    setup1();
    super.testGenericTorture8();
    setup2();
    super.testGenericTorture8();
    setup3();
    super.testGenericTorture8();
    setup4();
    super.testGenericTorture8();
  }

  @Test
  @Override
  public void testGenericTorture9() throws NameResolutionException {
    setup0();
    super.testGenericTorture9();
    setup1();
    super.testGenericTorture9();
    setup2();
    super.testGenericTorture9();
    setup3();
    super.testGenericTorture9();
    setup4();
    super.testGenericTorture9();
  }

  @Test
  @Override
  public void testGenericTorture10() throws NameResolutionException {
    setup0();
    super.testGenericTorture10();
    setup1();
    super.testGenericTorture10();
    setup2();
    super.testGenericTorture10();
    setup3();
    super.testGenericTorture10();
    setup4();
    super.testGenericTorture10();
  }

  @Test
  @Override
  public void testGenericTorture11() throws NameResolutionException {
    setup0();
    super.testGenericTorture11();
    setup1();
    super.testGenericTorture11();
    setup2();
    super.testGenericTorture11();
    setup3();
    super.testGenericTorture11();
    setup4();
    super.testGenericTorture11();
  }

  @Test
  @Override
  public void testGenericTorture12() throws NameResolutionException {
    setup0();
    super.testGenericTorture12();
    setup1();
    super.testGenericTorture12();
    setup2();
    super.testGenericTorture12();
    setup3();
    super.testGenericTorture12();
    setup4();
    super.testGenericTorture12();
  }

  @Test
  @Override
  public void testInjectNonStaticLocalArgClass() throws NameResolutionException {
    setup0();
    super.testInjectNonStaticLocalArgClass();
    setup1();
    super.testInjectNonStaticLocalArgClass();
    setup2();
    super.testInjectNonStaticLocalArgClass();
    setup3();
    super.testInjectNonStaticLocalArgClass();
    setup4();
    super.testInjectNonStaticLocalArgClass();
  }

  @Test
  @Override
  public void testOKShortNames() throws NameResolutionException {
    setup0();
    super.testOKShortNames();
    setup1();
    super.testOKShortNames();
    setup2();
    super.testOKShortNames();
    setup3();
    super.testOKShortNames();
    setup4();
    super.testOKShortNames();
  }

  @Test
  @Override
  public void testRoundTripInnerClassNames() throws NameResolutionException, ClassNotFoundException {
    setup0();
    super.testRoundTripInnerClassNames();
    setup1();
    super.testRoundTripInnerClassNames();
    setup2();
    super.testRoundTripInnerClassNames();
    setup3();
    super.testRoundTripInnerClassNames();
    setup4();
    super.testRoundTripInnerClassNames();
  }

  @Test
  @Override
  public void testUnitIsInjectable() throws NameResolutionException, InjectionException {
    setup0();
    super.testUnitIsInjectable();
    setup1();
    super.testUnitIsInjectable();
    setup2();
    super.testUnitIsInjectable();
    setup3();
    super.testUnitIsInjectable();
    setup4();
    super.testUnitIsInjectable();
  }

  @Test
  @Override
  public void testBadUnitDecl() throws NameResolutionException {
    setup0();
    super.testBadUnitDecl();
    setup1();
    super.testBadUnitDecl();
    setup2();
    super.testBadUnitDecl();
    setup3();
    super.testBadUnitDecl();
    setup4();
    super.testBadUnitDecl();
  }

  @Test
  @Override
  public void nameCantBindWrongSubclassAsDefault() throws NameResolutionException {
    setup0();
    super.nameCantBindWrongSubclassAsDefault();
    setup1();
    super.nameCantBindWrongSubclassAsDefault();
    setup2();
    super.nameCantBindWrongSubclassAsDefault();
    setup3();
    super.nameCantBindWrongSubclassAsDefault();
    setup4();
    super.nameCantBindWrongSubclassAsDefault();
  }

  @Test
  @Override
  public void ifaceCantBindWrongImplAsDefault() throws NameResolutionException {
    setup0();
    super.ifaceCantBindWrongImplAsDefault();
    setup1();
    super.ifaceCantBindWrongImplAsDefault();
    setup2();
    super.ifaceCantBindWrongImplAsDefault();
    setup3();
    super.ifaceCantBindWrongImplAsDefault();
    setup4();
    super.ifaceCantBindWrongImplAsDefault();
  }
}
