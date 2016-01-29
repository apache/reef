// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Collections.Generic;
using System.Reflection;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.InjectionPlan;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Implementations.ClassHierarchy
{
    internal static class NodeFactory
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(NodeFactory));

        public static IPackageNode CreateRootPackageNode()
        {
            return new PackageNodeImpl();
        }

        public static INode CreateClassNode(INode parent, Type clazz)
        {
            // var namedParameter = clazz.GetCustomAttribute<NamedParameterAttribute>();
            var unit = null != clazz.GetCustomAttribute<UnitAttribute>();
            string simpleName = ReflectionUtilities.GetName(clazz);
            string fullName = ReflectionUtilities.GetAssemblyQualifiedName(clazz);

            //// bool isStatic = true; // clazz.IsSealed && clazz.IsAbstract; always true in C# for Java static class
            //// bool injectable = true; // always true in C#

            bool isAssignableFromExternalConstructor = ReflectionUtilities.IsAssignableFromIgnoreGeneric(typeof(IExternalConstructor<>), clazz); 

            // bool parentIsUnit = false; 

            ////No such thing in C#, should be false
            ////bool foundNonStaticInnerClass = false;
            ////foreach (Type c in clazz.getNestedTypes()) {
            ////  if (!Modifier.isStatic(c.getModifiers())) {
            ////    foundNonStaticInnerClass = true;
            ////  }
            ////}

            var injectableConstructors = new List<IConstructorDef>();
            var allConstructors = new List<IConstructorDef>();

            foreach (ConstructorInfo c in clazz.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))  
            {
                var constructorAnnotatedInjectable = null != c.GetCustomAttribute<InjectAttribute>();

                bool constructorInjectable = constructorAnnotatedInjectable;

                ConstructorDefImpl constructorDef = CreateConstructorDef(c, constructorAnnotatedInjectable);

                if (constructorInjectable)
                {
                    // if (injectableConstructors.Contains(constructorDef))
                    if (constructorDef.IsInList(injectableConstructors))
                    {
                        var e = new ClassHierarchyException(
                            "Ambiguous boundConstructors detected in class " + clazz + ": "
                                + constructorDef + " differs from some other" + " constructor only "
                                + "by parameter order.");
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                    }
                    else
                    {
                        injectableConstructors.Add(constructorDef);
                    }
                }
                allConstructors.Add(constructorDef);
            }

            string defaultImplementation = null;
            DefaultImplementationAttribute defaultImpl = clazz.GetCustomAttribute<DefaultImplementationAttribute>();
            if (null != defaultImpl)
            {
                Type defaultImplementationClazz = defaultImpl.Value;

                if (defaultImplementationClazz == null)
                {
                    defaultImplementation = defaultImpl.Name;
                }
                else
                {
                    if (!ReflectionUtilities.IsAssignableFromIgnoreGeneric(clazz, defaultImplementationClazz))
                    {
                        var e = new ClassHierarchyException(clazz
                                                          + " declares its default implementation to be non-subclass "
                                                          + defaultImplementationClazz);
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                    }
                    defaultImplementation = ReflectionUtilities.GetAssemblyQualifiedName(defaultImplementationClazz);
                }
            }
            else
            {
                defaultImplementation = null;
            }        

            return new ClassNodeImpl(parent, simpleName, fullName, unit, true, isAssignableFromExternalConstructor, 
                injectableConstructors, allConstructors, defaultImplementation);
        }

        // TODO
        private static ConstructorDefImpl CreateConstructorDef(ConstructorInfo constructor, bool injectable)
        {
            var parameters = constructor.GetParameters();

            IConstructorArg[] args = new ConstructorArgImpl[parameters.Length];

            for (int i = 0; i < parameters.Length; i++)
            {
                // TODO for getInterfaceTarget() call
                Type type = parameters[i].ParameterType;
                type = ReflectionUtilities.EnsureInterfaceType(type);
                ////if (type.IsGenericType && type.FullName == null)
                ////{
                ////   type = type.GetGenericTypeDefinition();
                ////}
                bool isFuture;

                if (ReflectionUtilities.IsAssignableFromIgnoreGeneric(typeof(IInjectionFuture<>), type)) 
                {
                    type = ReflectionUtilities.GetInterfaceTarget(typeof(IInjectionFuture<>), type);
                    isFuture = true;
                } 
                else 
                {
                    isFuture = false;
                }

                ParameterAttribute named = parameters[i].GetCustomAttribute<ParameterAttribute>();
                if (named != null && !injectable)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ClassHierarchyException(constructor + " is not injectable, but it has an @Parameter annotation."), LOGGER);
                }

                if (type == null)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ApplicationException("Exception"), LOGGER);
                }

                string typename = ReflectionUtilities.GetAssemblyQualifiedName(type);
                if (typename == null)
                {
                    typename = type.Name;
                }
                args[i] = new ConstructorArgImpl(typename, named == null ? null : ReflectionUtilities.GetAssemblyQualifiedName(named.Value), isFuture);
            }
            return new ConstructorDefImpl(ReflectionUtilities.GetAssemblyQualifiedName(constructor.DeclaringType), args, injectable); 
        }

        public static INamedParameterNode CreateNamedParameterNode(INode parent, Type clazz, Type argClass)
        {
            Type setRawArgType = ReflectionUtilities.GetInterfaceTarget(typeof(ISet<>), argClass);
            bool isSet = setRawArgType != null;
            if (isSet) 
            {
                argClass = setRawArgType;
            }

            Type listRawArgType = ReflectionUtilities.GetInterfaceTargetForType(typeof(IList<>), argClass);
            bool isList = listRawArgType != null;
            if (isList)
            {
                argClass = listRawArgType;
            }

            string simpleName = ReflectionUtilities.GetName(clazz);
            string fullName = ReflectionUtilities.GetAssemblyQualifiedName(clazz);
            string fullArgName = ReflectionUtilities.GetAssemblyQualifiedName(argClass);
            string simpleArgName = ReflectionUtilities.GetName(argClass);

            NamedParameterAttribute namedParameter = clazz.GetCustomAttribute<NamedParameterAttribute>();

            if (namedParameter == null)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException("Got name without named parameter post-validation!"), LOGGER);
            }

            bool hasStringDefault, hasClassDefault, hasStringSetDefault, hasClassSetDefault;
            int default_count = 0;
            if (namedParameter.DefaultValue == null || namedParameter.DefaultValue.Equals(NamedParameterAttribute.ReefUninitializedValue))
            {
                hasStringDefault = false;
            }
            else
            {
                hasStringDefault = true;
                default_count++;
            }

            if (namedParameter.DefaultClass != null /*Void.class*/)
            {
                hasClassDefault = true;
                default_count++;
            }
            else
            {
                hasClassDefault = false;
            }

            if (namedParameter.DefaultValues != null && namedParameter.DefaultValues.Length > 0)
            {
                hasStringSetDefault = true;
                default_count++;
            }
            else
            {
                hasStringSetDefault = false;
            }

            if (namedParameter.DefaultClasses != null && namedParameter.DefaultClasses.Length > 0) 
            {
                hasClassSetDefault = true;
                default_count++;
            }
            else
            {
                hasClassSetDefault = false;
            }

            if (default_count > 1)
            {
                var e = new ClassHierarchyException("Named parameter " + fullName + " defines more than one of default_value, default_class, default_values and default_classes");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
            }

            string[] defaultInstanceAsStrings = new string[] { };

            if (default_count == 0)
            {
                defaultInstanceAsStrings = new string[] { };
            }
            else if (hasClassDefault)
            {
                Type default_class = namedParameter.DefaultClass;
                AssertIsSubclassOf(clazz, default_class, argClass);
                defaultInstanceAsStrings = new string[] { ReflectionUtilities.GetAssemblyQualifiedName(default_class) };
            }
            else if (hasStringDefault)
            {
                // Don't know if the string is a class or literal here, so don't bother validating.
                defaultInstanceAsStrings = new string[] { namedParameter.DefaultValue };
            }
            else if (hasClassSetDefault)
            {
                Type[] clzs = namedParameter.DefaultClasses;
                defaultInstanceAsStrings = new string[clzs.Length];
                for (int i = 0; i < clzs.Length; i++)
                {
                    AssertIsSubclassOf(clazz, clzs[i], argClass);
                    defaultInstanceAsStrings[i] = ReflectionUtilities.GetAssemblyQualifiedName(clzs[i]);
                }
            }
            else if (hasStringSetDefault)
            {
                defaultInstanceAsStrings = namedParameter.DefaultValues;
            }
            else
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException(), LOGGER);
            }

            string documentation = namedParameter.Documentation;
            string shortName = namedParameter.ShortName;
            if (namedParameter.ShortName != null && namedParameter.ShortName.Length == 0)
            {
                shortName = null;
            }

            return new NamedParameterNodeImpl(parent, simpleName, fullName,
                fullArgName, simpleArgName, isSet, isList, documentation, shortName, defaultInstanceAsStrings, namedParameter.Alias, namedParameter.AliasLanguage);
        }

        // private static void assertIsSubclassOf(Class<?> named_parameter, Class<?> default_class, Type argClass) {
        private static void AssertIsSubclassOf(Type namedparameter, Type defaultclass, Type argClass)
        {
            bool isSubclass = false;
            string argClassName = ReflectionUtilities.GetAssemblyQualifiedName(argClass);
            foreach (Type t in ReflectionUtilities.ClassAndAncestors(defaultclass))
            {
                if (argClassName.Equals(ReflectionUtilities.GetAssemblyQualifiedName(t)))
                {
                    isSubclass = true;
                }
            }
            if (!isSubclass)
            {
                var e = new ClassHierarchyException(namedparameter + " defines a default class "
                    + ReflectionUtilities.GetName(defaultclass) + " with a type that does not extend of its target's type " + argClass);
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
            }
        }
    }
}
