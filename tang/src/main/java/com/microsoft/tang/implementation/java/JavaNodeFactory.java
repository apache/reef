/**
 * Copyright (C) 2012 Microsoft Corporation
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
package com.microsoft.tang.implementation.java;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import javax.inject.Inject;

import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.ClassHierarchyException;
import com.microsoft.tang.implementation.types.ClassNodeImpl;
import com.microsoft.tang.implementation.types.ConstructorArgImpl;
import com.microsoft.tang.implementation.types.ConstructorDefImpl;
import com.microsoft.tang.implementation.types.NamedParameterNodeImpl;
import com.microsoft.tang.implementation.types.PackageNodeImpl;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorArg;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.types.PackageNode;
import com.microsoft.tang.util.MonotonicSet;
import com.microsoft.tang.util.ReflectionUtilities;

public class JavaNodeFactory {

  @SuppressWarnings("unchecked")
  static <T> ClassNodeImpl<T> createClassNode(Node parent, Class<T> clazz) throws ClassHierarchyException {
    final boolean injectable;
    final boolean unit = clazz.isAnnotationPresent(Unit.class);
    final String simpleName = ReflectionUtilities.getSimpleName(clazz);
    final String fullName = ReflectionUtilities.getFullName(clazz);
    final boolean isStatic = Modifier.isStatic(clazz.getModifiers()); 
    final boolean parentIsUnit = ((parent instanceof ClassNode) && !isStatic) ?
        ((ClassNode<?>)parent).isUnit() : false;

    if (clazz.isLocalClass() || clazz.isMemberClass()) {
      if (!isStatic) {
        if(parent instanceof ClassNode) {
          injectable = ((ClassNode<?>)parent).isUnit();
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
    for(Class<?> c : clazz.getDeclaredClasses()) {
      if(!Modifier.isStatic(c.getModifiers())) {
        foundNonStaticInnerClass = true;
      }
    }
    
    if(unit && !foundNonStaticInnerClass) {
      throw new ClassHierarchyException("Class " + ReflectionUtilities.getFullName(clazz) + " has an @Unit annotation, but no non-static inner classes.  Such @Unit annotations would have no effect, and are therefore disallowed.");
    }
    
    Constructor<T>[] constructors = (Constructor<T>[]) clazz
        .getDeclaredConstructors();
    MonotonicSet<ConstructorDef<T>> injectableConstructors = new MonotonicSet<>();
    ArrayList<ConstructorDef<T>> allConstructors = new ArrayList<>();
    for (int k = 0; k < constructors.length; k++) {
      boolean constructorAnnotatedInjectable = (constructors[k]
          .getAnnotation(Inject.class) != null);
      if (constructorAnnotatedInjectable && constructors[k].isSynthetic()) {
        // Not sure if we *can* unit test this one.
        throw new ClassHierarchyException(
            "Synthetic constructor was annotated with @Inject!");
      }
      if (parentIsUnit && (constructorAnnotatedInjectable || constructors[k].getParameterTypes().length != 1)) {
        throw new ClassHierarchyException(
            "Detected explicit constructor in class enclosed in @Unit " + fullName + "  Such constructors are disallowed.");
      }
      boolean constructorInjectable = constructorAnnotatedInjectable || parentIsUnit;
      // ConstructorDef's constructor checks for duplicate
      // parameters
      // The injectableConstructors set checks for ambiguous
      // boundConstructors.
      ConstructorDef<T> def = JavaNodeFactory.createConstructorDef(injectable,
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
      DefaultImplementation defaultImpl
        = clazz.getAnnotation(DefaultImplementation.class);
      Class<?> defaultImplementationClazz = defaultImpl.value();
      if(defaultImplementationClazz.equals(Void.class)) {
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
  public static <T> NamedParameterNode<T> createNamedParameterNode(Node parent,
      Class<? extends Name<T>> clazz, Type argClass) throws ClassHierarchyException {

    Class<?> argRawClass = ReflectionUtilities.getRawClass(argClass);
    
    final boolean isSet = argRawClass.equals(Set.class);

    
    if(isSet) {
      argClass = ReflectionUtilities.getInterfaceTarget(Collection.class, argClass);
      argRawClass = ReflectionUtilities.getRawClass(argClass);
    }
    
    final String simpleName = ReflectionUtilities.getSimpleName(clazz);
    final String fullName = ReflectionUtilities.getFullName(clazz);
    final String fullArgName = ReflectionUtilities.getFullName(argClass);
    final String simpleArgName = ReflectionUtilities.getSimpleName(argClass);

    
    final NamedParameter namedParameter = clazz.getAnnotation(NamedParameter.class);

    if (namedParameter == null) {
      throw new IllegalStateException("Got name without named parameter post-validation!");
    }
    final boolean hasStringDefault, hasClassDefault, hasStringSetDefault, hasClassSetDefault;
    
    int default_count = 0;
    if(!namedParameter.default_value().isEmpty()) {
      hasStringDefault = true;
      default_count++;
    } else {
      hasStringDefault = false;
    }
    if(namedParameter.default_class() != Void.class) {
      hasClassDefault = true;
      default_count++;
    } else {
      hasClassDefault = false;
    }
    if(namedParameter.default_values() != null && namedParameter.default_values().length > 0) {
      hasStringSetDefault = true;
      default_count++;
    } else {
      hasStringSetDefault = false;
    }
    if(namedParameter.default_classes() != null && namedParameter.default_classes().length > 0) {
      hasClassSetDefault = true;
      default_count++;
    } else {
      hasClassSetDefault = false;
    }
    if(default_count > 1) {
      throw new ClassHierarchyException("Named parameter " + fullName + " defines more than one of default_value, default_class, default_values and default_classes");
    }
    
    final String[] defaultInstanceAsStrings;

    if (default_count == 0) {
      defaultInstanceAsStrings = new String[]{};
    } else if (hasClassDefault) {
      final Class<?> default_class = namedParameter.default_class();
      assertIsSubclassOf(clazz, default_class, argClass);
      defaultInstanceAsStrings = new String[] {ReflectionUtilities.getFullName(default_class)};
    } else if(hasStringDefault) {
      // Don't know if the string is a class or literal here, so don't bother validating.
      defaultInstanceAsStrings = new String[] { namedParameter.default_value() };
    } else if(hasClassSetDefault) {
      Class<?>[] clzs = namedParameter.default_classes();
      defaultInstanceAsStrings = new String[clzs.length];
      for(int i = 0; i < clzs.length; i++) {
        assertIsSubclassOf(clazz, clzs[i], argClass);
        defaultInstanceAsStrings[i] = ReflectionUtilities.getFullName(clzs[i]);
      }
    } else if(hasStringSetDefault) {
      defaultInstanceAsStrings = namedParameter.default_values();
    } else {
      throw new IllegalStateException();
    }

    final String documentation = namedParameter.doc();
    
    final String shortName = namedParameter.short_name().isEmpty()
        ? null : namedParameter.short_name();

    return new NamedParameterNodeImpl<>(parent, simpleName, fullName,
        fullArgName, simpleArgName, isSet, documentation, shortName, defaultInstanceAsStrings);
  }

  private static void assertIsSubclassOf(Class<?> named_parameter, Class<?> default_class,
      Type argClass) {
    boolean isSubclass = false;
    boolean isGenericSubclass= false;
    Class<?> argRawClass = ReflectionUtilities.getRawClass(argClass);
    
    // Note: We intentionally strip the raw type information here.  The reason is to handle
    // EventHandler-style patterns and collections.
    
    /// If we have a Name that takes EventHandler<A>, we want to be able to pass in an EventHandler<Object>.
    
    for (final Type c : ReflectionUtilities.classAndAncestors(default_class)) {
      if (ReflectionUtilities.getRawClass(c).equals(argRawClass)) {
        isSubclass = true;
        if(argClass instanceof ParameterizedType &&
           c instanceof ParameterizedType) {
          ParameterizedType argPt = (ParameterizedType)argClass;
          ParameterizedType defaultPt = (ParameterizedType)c;
          
          Class<?> rawDefaultParameter = ReflectionUtilities.getRawClass(defaultPt.getActualTypeArguments()[0]);
          Class<?> rawArgParameter = ReflectionUtilities.getRawClass(argPt.getActualTypeArguments()[0]);
          
          for (final Type d: ReflectionUtilities.classAndAncestors(argPt.getActualTypeArguments()[0])) {
            if(ReflectionUtilities.getRawClass(d).equals(rawDefaultParameter)) {
              isGenericSubclass = true;
            }
          }
          for (final Type d: ReflectionUtilities.classAndAncestors(defaultPt.getActualTypeArguments()[0])) {
            if(ReflectionUtilities.getRawClass(d).equals(rawArgParameter)) {
              isGenericSubclass = true;
            }
          }
        } else {
          isGenericSubclass = true;
        }
      }
    }

    if (!(isSubclass)) {
      throw new ClassHierarchyException(named_parameter + " defines a default class "
          + ReflectionUtilities.getFullName(default_class) + " with a raw type that does not extend of its target's raw type " + argRawClass);
    }
    if (!(isGenericSubclass)) {
      throw new ClassHierarchyException(named_parameter + " defines a default class "
          + ReflectionUtilities.getFullName(default_class) + " with a type that does not extend its target's type " + argClass);
    }
  }
  public static PackageNode createRootPackageNode() {
    return new PackageNodeImpl();
  }

  private static <T> ConstructorDef<T> createConstructorDef(
      boolean isClassInjectionCandidate, Constructor<T> constructor,
      boolean injectable) throws ClassHierarchyException {
    // We don't support injection of non-static member classes with @Inject
    // annotations.
    if (injectable && !isClassInjectionCandidate) {
      throw new ClassHierarchyException("Cannot @Inject non-static member class unless the enclosing class an @Unit.  Nested class is:"
          + ReflectionUtilities.getFullName(constructor.getDeclaringClass()));
    }
    // TODO: When we use paramTypes below, we strip generic parameters.  Is that OK?
    Class<?>[] paramTypes = constructor.getParameterTypes();
    Type[] genericParamTypes = constructor.getGenericParameterTypes();
    Annotation[][] paramAnnotations = constructor.getParameterAnnotations();
    if (paramTypes.length != paramAnnotations.length) {
      throw new IllegalStateException();
    }
    ConstructorArg[] args = new ConstructorArg[genericParamTypes.length];
    for (int i = 0; i < genericParamTypes.length; i++) {
      // If this parameter is an injection future, unwrap the target class,
      // and remember by setting isFuture to true.
      final Type type;
      final boolean isFuture;
      if(InjectionFuture.class.isAssignableFrom(paramTypes[i])) {
        type = ReflectionUtilities.getInterfaceTarget(InjectionFuture.class, genericParamTypes[i]);
        isFuture = true;
      } else {
        type = paramTypes[i];
        isFuture = false;
      }
      // Make node of the named parameter annotation (if any).
      Parameter named = null;
      for (int j = 0; j < paramAnnotations[i].length; j++) {
        Annotation annotation = paramAnnotations[i][j];
        if (annotation instanceof Parameter) {
          if((!isClassInjectionCandidate) || !injectable) {
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

}
