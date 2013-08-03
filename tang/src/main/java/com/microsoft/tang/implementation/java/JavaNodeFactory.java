package com.microsoft.tang.implementation.java;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
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
    final boolean parentIsUnit = (parent instanceof ClassNode) ?
        ((ClassNode<?>)parent).isUnit() : false;

    if (clazz.isLocalClass() || clazz.isMemberClass()) {
      if (!Modifier.isStatic(clazz.getModifiers())) {
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
                  + def + " differs from some other " + " constructor only "
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

  public static <T> NamedParameterNode<T> createNamedParameterNode(Node parent,
      Class<? extends Name<T>> clazz, Type argClass) throws ClassHierarchyException {

    Class<?> argRawClass = ReflectionUtilities.getRawClass(argClass);
    
    final boolean isSet = argRawClass.equals(Set.class);

    
    if(isSet) {
      argClass = ReflectionUtilities.getInterfaceTarget(Collection.class, argClass);
    }
    
    final String simpleName = ReflectionUtilities.getSimpleName(clazz);
    final String fullName = ReflectionUtilities.getFullName(clazz);
    final String fullArgName = ReflectionUtilities.getFullName(argClass);
    final String simpleArgName = ReflectionUtilities.getSimpleName(argClass);

    
    final NamedParameter namedParameter = clazz.getAnnotation(NamedParameter.class);

    if (namedParameter == null) {
      throw new IllegalStateException("Got name without named parameter post-validation!");
    }
    
    final boolean hasStringDefault = !namedParameter.default_value().isEmpty();
    final boolean hasClassDefault = namedParameter.default_class() != Void.class;
    
    final String defaultInstanceAsString;

    if (hasStringDefault && hasClassDefault) {
      throw new ClassHierarchyException("Named parameter " + fullName +
          " declares both a default_value and default_class.  At most one is allowed.");
    } else if (!(hasStringDefault || hasClassDefault)) {
      defaultInstanceAsString = null;
    } else if (namedParameter.default_class() != Void.class) {
      defaultInstanceAsString = ReflectionUtilities.getFullName(namedParameter.default_class());
      boolean isSubclass = false;
      for (final Class<?> c : ReflectionUtilities.classAndAncestors(namedParameter.default_class())) {
        if (c.equals(argClass)) {
          isSubclass = true;
          break;
        }
      }
      if (!isSubclass) {
        throw new ClassHierarchyException(clazz + " defines a default class "
            + defaultInstanceAsString + " that is not an instance of its target " + argClass);
      }
    } else {
      defaultInstanceAsString = namedParameter.default_value();
      // Don't know if the string is a class or literal here, so don't bother validating.
    }

    final String documentation = namedParameter.doc();
    
    final String shortName = namedParameter.short_name().isEmpty()
        ? null : namedParameter.short_name();

    return new NamedParameterNodeImpl<>(parent, simpleName, fullName,
        fullArgName, simpleArgName, isSet, documentation, shortName, defaultInstanceAsString);
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
