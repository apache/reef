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
package org.apache.reef.tang.implementation.java;

import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.*;
import org.apache.reef.tang.exceptions.ClassHierarchyException;
import org.apache.reef.tang.implementation.types.*;
import org.apache.reef.tang.types.*;
import org.apache.reef.tang.util.MonotonicSet;
import org.apache.reef.tang.util.ReflectionUtilities;

import javax.inject.Inject;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public final class JavaNodeFactory {

  @SuppressWarnings("unchecked")
  static <T> ClassNodeImpl<T> createClassNode(final Node parent, final Class<T> clazz) throws ClassHierarchyException {
    final boolean injectable;
    final boolean unit = clazz.isAnnotationPresent(Unit.class);
    final String simpleName = ReflectionUtilities.getSimpleName(clazz);
    final String fullName = ReflectionUtilities.getFullName(clazz);
    final boolean isStatic = Modifier.isStatic(clazz.getModifiers());
    final boolean parentIsUnit = parent instanceof ClassNode && !isStatic && ((ClassNode<?>) parent).isUnit();

    if (clazz.isLocalClass() || clazz.isMemberClass()) {
      if (!isStatic) {
        if (parent instanceof ClassNode) {
          injectable = ((ClassNode<?>) parent).isUnit();
        } else {
          injectable = false;
        }
      } else {
        injectable = true;
      }
    } else {
      injectable = true;
    }

    boolean foundNonStaticInnerClass = false;
    for (final Class<?> c : clazz.getDeclaredClasses()) {
      if (!Modifier.isStatic(c.getModifiers())) {
        foundNonStaticInnerClass = true;
      }
    }

    if (unit && !foundNonStaticInnerClass) {
      throw new ClassHierarchyException("Class " + ReflectionUtilities.getFullName(clazz) +
          " has an @Unit annotation, but no non-static inner classes. " +
          " Such @Unit annotations would have no effect, and are therefore disallowed.");
    }

    final Constructor<T>[] constructors = (Constructor<T>[]) clazz
        .getDeclaredConstructors();
    final MonotonicSet<ConstructorDef<T>> injectableConstructors = new MonotonicSet<>();
    final ArrayList<ConstructorDef<T>> allConstructors = new ArrayList<>();
    for (int k = 0; k < constructors.length; k++) {
      final boolean constructorAnnotatedInjectable = constructors[k].getAnnotation(Inject.class) != null;
      if (constructorAnnotatedInjectable && constructors[k].isSynthetic()) {
        // Not sure if we *can* unit test this one.
        throw new ClassHierarchyException(
            "Synthetic constructor was annotated with @Inject!");
      }
      if (parentIsUnit && (constructorAnnotatedInjectable || constructors[k].getParameterTypes().length != 1)) {
        throw new ClassHierarchyException(
            "Detected explicit constructor in class enclosed in @Unit " + fullName +
                "  Such constructors are disallowed.");
      }
      final boolean constructorInjectable = constructorAnnotatedInjectable || parentIsUnit;
      // ConstructorDef's constructor checks for duplicate
      // parameters
      // The injectableConstructors set checks for ambiguous
      // boundConstructors.
      final ConstructorDef<T> def = JavaNodeFactory.createConstructorDef(injectable,
          constructors[k], constructorAnnotatedInjectable);
      if (constructorInjectable) {
        if (injectableConstructors.contains(def)) {
          throw new ClassHierarchyException(
              "Ambiguous boundConstructors detected in class " + clazz + ": "
                  + def + " differs from some other" + " constructor only "
                  + "by parameter order.");
        } else {
          injectableConstructors.add(def);
        }
      }
      allConstructors.add(def);
    }
    final String defaultImplementation;
    if (clazz.isAnnotationPresent(DefaultImplementation.class)) {
      final DefaultImplementation defaultImpl
          = clazz.getAnnotation(DefaultImplementation.class);
      final Class<?> defaultImplementationClazz = defaultImpl.value();
      if (defaultImplementationClazz.equals(Void.class)) {
        defaultImplementation = defaultImpl.name();
        // XXX check isAssignableFrom, other type problems here.
      } else {
        if (!clazz.isAssignableFrom(defaultImplementationClazz)) {
          throw new ClassHierarchyException(clazz
              + " declares its default implementation to be non-subclass "
              + defaultImplementationClazz);
        }
        defaultImplementation = ReflectionUtilities.getFullName(defaultImplementationClazz);
      }
    } else {
      defaultImplementation = null;
    }

    return new ClassNodeImpl<T>(parent, simpleName, fullName, unit, injectable,
        ExternalConstructor.class.isAssignableFrom(clazz),
        injectableConstructors.toArray(new ConstructorDefImpl[0]),
        allConstructors.toArray(new ConstructorDefImpl[0]), defaultImplementation);
  }

  /**
   * XXX: This method assumes that all generic types have exactly one type parameter.
   */
  public static <T> NamedParameterNode<T> createNamedParameterNode(final Node parent,
                                                                   final Class<? extends Name<T>> clazz,
                                                                   final Type argClass)
      throws ClassHierarchyException {

    Class<?> argRawClass = ReflectionUtilities.getRawClass(argClass);

    final boolean isSet = argRawClass.equals(Set.class);
    final boolean isList = argRawClass.equals(List.class);

    final Type argClazz;

    if (isSet || isList) {
      argClazz = ReflectionUtilities.getInterfaceTarget(Collection.class, argClass);
    } else {
      argClazz = argClass;
    }

    final String simpleName = ReflectionUtilities.getSimpleName(clazz);
    final String fullName = ReflectionUtilities.getFullName(clazz);
    final String fullArgName = ReflectionUtilities.getFullName(argClazz);
    final String simpleArgName = ReflectionUtilities.getSimpleName(argClazz);


    final NamedParameter namedParameter = clazz.getAnnotation(NamedParameter.class);

    if (namedParameter == null) {
      throw new IllegalStateException("Got name without named parameter post-validation!");
    }
    final boolean hasStringDefault, hasClassDefault, hasStringSetDefault, hasClassSetDefault;

    int defaultCount = 0;
    if (namedParameter.default_value().equals(NamedParameter.REEF_UNINITIALIZED_VALUE)) {
      hasStringDefault = false;
    } else {
      hasStringDefault = true;
      defaultCount++;
    }
    if (namedParameter.default_class() != Void.class) {
      hasClassDefault = true;
      defaultCount++;
    } else {
      hasClassDefault = false;
    }
    if (namedParameter.default_values() != null && namedParameter.default_values().length > 0) {
      hasStringSetDefault = true;
      defaultCount++;
    } else {
      hasStringSetDefault = false;
    }
    if (namedParameter.default_classes() != null && namedParameter.default_classes().length > 0) {
      hasClassSetDefault = true;
      defaultCount++;
    } else {
      hasClassSetDefault = false;
    }
    if (defaultCount > 1) {
      throw new ClassHierarchyException("Named parameter " + fullName +
          " defines more than one of default_value, default_class, default_values and default_classes");
    }

    final String[] defaultInstanceAsStrings;

    if (defaultCount == 0) {
      defaultInstanceAsStrings = new String[]{};
    } else if (hasClassDefault) {
      final Class<?> defaultClass = namedParameter.default_class();
      assertIsSubclassOf(clazz, defaultClass, argClazz);
      defaultInstanceAsStrings = new String[]{ReflectionUtilities.getFullName(defaultClass)};
    } else if (hasStringDefault) {
      // Don't know if the string is a class or literal here, so don't bother validating.
      defaultInstanceAsStrings = new String[]{namedParameter.default_value()};
    } else if (hasClassSetDefault) {
      final Class<?>[] clzs = namedParameter.default_classes();
      defaultInstanceAsStrings = new String[clzs.length];
      for (int i = 0; i < clzs.length; i++) {
        assertIsSubclassOf(clazz, clzs[i], argClazz);
        defaultInstanceAsStrings[i] = ReflectionUtilities.getFullName(clzs[i]);
      }
    } else if (hasStringSetDefault) {
      defaultInstanceAsStrings = namedParameter.default_values();
    } else {
      throw new IllegalStateException("The named parameter " + namedParameter.short_name() + " has a default value,"
              + " but the value cannot be retrieved. The defaultCount is " + defaultCount
              + ", but hasClassDefault, hasStringDefault, hasClassSetDefault, hasStringSetDefault"
              + " conditions are all false");
    }

    final String documentation = namedParameter.doc();

    final String shortName = namedParameter.short_name().isEmpty()
        ? null : namedParameter.short_name();

    return new NamedParameterNodeImpl<>(parent, simpleName, fullName,
        fullArgName, simpleArgName, isSet, isList, documentation, shortName, defaultInstanceAsStrings);
  }

  private static void assertIsSubclassOf(final Class<?> namedParameter, final Class<?> defaultClass,
                                         final Type argClass) {
    boolean isSubclass = false;
    boolean isGenericSubclass = false;
    final Class<?> argRawClass = ReflectionUtilities.getRawClass(argClass);

    // Note: We intentionally strip the raw type information here.  The reason is to handle
    // EventHandler-style patterns and collections.

    /// If we have a Name that takes EventHandler<A>, we want to be able to pass in an EventHandler<Object>.

    for (final Type c : ReflectionUtilities.classAndAncestors(defaultClass)) {
      if (ReflectionUtilities.getRawClass(c).equals(argRawClass)) {
        isSubclass = true;
        if (argClass instanceof ParameterizedType &&
            c instanceof ParameterizedType) {
          final ParameterizedType argPt = (ParameterizedType) argClass;
          final ParameterizedType defaultPt = (ParameterizedType) c;

          final Class<?> rawDefaultParameter = ReflectionUtilities.getRawClass(defaultPt.getActualTypeArguments()[0]);
          final Class<?> rawArgParameter = ReflectionUtilities.getRawClass(argPt.getActualTypeArguments()[0]);

          for (final Type d : ReflectionUtilities.classAndAncestors(argPt.getActualTypeArguments()[0])) {
            if (ReflectionUtilities.getRawClass(d).equals(rawDefaultParameter)) {
              isGenericSubclass = true;
            }
          }
          for (final Type d : ReflectionUtilities.classAndAncestors(defaultPt.getActualTypeArguments()[0])) {
            if (ReflectionUtilities.getRawClass(d).equals(rawArgParameter)) {
              isGenericSubclass = true;
            }
          }
        } else {
          isGenericSubclass = true;
        }
      }
    }

    if (!isSubclass) {
      throw new ClassHierarchyException(namedParameter + " defines a default class "
          + ReflectionUtilities.getFullName(defaultClass)
          + " with a raw type that does not extend of its target's raw type " + argRawClass);
    }
    if (!isGenericSubclass) {
      throw new ClassHierarchyException(namedParameter + " defines a default class "
          + ReflectionUtilities.getFullName(defaultClass)
          + " with a type that does not extend its target's type " + argClass);
    }
  }

  public static PackageNode createRootPackageNode() {
    return new PackageNodeImpl();
  }

  private static <T> ConstructorDef<T> createConstructorDef(
      final boolean isClassInjectionCandidate, final Constructor<T> constructor,
      final boolean injectable) throws ClassHierarchyException {
    // We don't support injection of non-static member classes with @Inject
    // annotations.
    if (injectable && !isClassInjectionCandidate) {
      throw new ClassHierarchyException("Cannot @Inject non-static member class unless the enclosing class an @Unit. "
          + " Nested class is:"
          + ReflectionUtilities.getFullName(constructor.getDeclaringClass()));
    }
    // TODO: When we use paramTypes below, we strip generic parameters.  Is that OK?
    final Class<?>[] paramTypes = constructor.getParameterTypes();
    final Type[] genericParamTypes = constructor.getGenericParameterTypes();
    final Annotation[][] paramAnnotations = constructor.getParameterAnnotations();
    if (paramTypes.length != paramAnnotations.length) {
      throw new IllegalStateException("The paramTypes.length is " + paramTypes.length
              + ", and paramAnnotations.length is " + paramAnnotations.length
              + ". These values should be equal."
      );
    }
    final ConstructorArg[] args = new ConstructorArg[genericParamTypes.length];
    for (int i = 0; i < genericParamTypes.length; i++) {
      // If this parameter is an injection future, unwrap the target class,
      // and remember by setting isFuture to true.
      final Type type;
      final boolean isFuture;
      if (InjectionFuture.class.isAssignableFrom(paramTypes[i])) {
        type = ReflectionUtilities.getInterfaceTarget(InjectionFuture.class, genericParamTypes[i]);
        isFuture = true;
      } else {
        type = paramTypes[i];
        isFuture = false;
      }
      // Make node of the named parameter annotation (if any).
      Parameter named = null;
      for (int j = 0; j < paramAnnotations[i].length; j++) {
        final Annotation annotation = paramAnnotations[i][j];
        if (annotation instanceof Parameter) {
          if (!isClassInjectionCandidate || !injectable) {
            throw new ClassHierarchyException(constructor + " is not injectable, but it has an @Parameter annotation.");
          }
          named = (Parameter) annotation;
        }
      }
      args[i] = new ConstructorArgImpl(
          ReflectionUtilities.getFullName(type), named == null ? null
          : ReflectionUtilities.getFullName(named.value()),
          isFuture);
    }
    return new ConstructorDefImpl<T>(
        ReflectionUtilities.getFullName(constructor.getDeclaringClass()),
        args, injectable);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private JavaNodeFactory() {
  }
}
