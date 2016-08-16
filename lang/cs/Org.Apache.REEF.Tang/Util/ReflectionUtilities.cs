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
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Util
{
    public static class ReflectionUtilities
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ReflectionUtilities));

        public static readonly string Regexp = "[\\.\\+]";

        /// <summary>
        /// Gets the AssemblyQualifiedName from the Type. This name is used in ClassHierarchy
        /// as a key when add a node as a child to parent. THe name is used as FullName in a Node
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentException">null is passed in FullName() in ReflectionUtilities</exception>
        public static string GetAssemblyQualifiedName(Type name)
        {
            if (name == null)
            {
                Utilities.Diagnostics.Exceptions.Throw(new ArgumentException("null is passed in FullName() in ReflectionUtilities"), LOGGER);
            }

            Type t = EnsureInterfaceType(name);

            if (t.AssemblyQualifiedName == null && t.Name == null && t.FullName == null)
            {
                LOGGER.Log(Level.Warning, "The type's name is null: " + t.ToString());
            }

            if (t.AssemblyQualifiedName == null && t.Name != null)
            {
                    return t.Name;
            }

            return t.AssemblyQualifiedName;
        }

        /// <summary>
        /// It returns Type.FullName. This name is used as Name in a Node. 
        /// It is not unique for a generic type with different type of arguments.
        /// It is used for toString or debug info as AssemblyQualifiedName is really long
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns></returns>
        public static string GetName(Type name)
        {
            if (name.FullName != null)
            {
                return name.FullName;
            }

            // The following lines should be not reached by C# syntax definition. However, it happens  for some generic type such as AbstractObserver<T>
            // It results in name as null. When null name in the class node gets deserialized, as name is required filed in class hierarchy proto buffer schema,
            // it causes exception during deserialization. The code below is to use first portion of AssemblyQualifiedName for the name of the node node in case type.name is null. 
            string[] parts = GetAssemblyQualifiedName(name).Split(',');
            return parts[0];
        }

        /// <summary>
        /// Gets the interface target.
        /// Foo<T> ,  given Foo<T> and Foo return T
        /// example class Foo : Bar<U>, Bas<T>
        /// iface: Bar, type: Foo, return U
        /// iface: Bas, type: Foo, return T
        /// class ACons implements IExternalConstructor<A> 
        /// iface: IExternalConstructor<>, type: ACons return A
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static Type GetInterfaceTarget(Type iface, Type type)
        {
            foreach (Type t in ReflectionUtilities.ClassAndAncestors(type))
            {
                if (IsGenericTypeof(iface, t))
                {
                    return t.GetGenericArguments()[0]; // verify it
                }
            }
            return null;
        }

        public static Type GetInterfaceTargetForType(Type iface, Type type)
        {
            if (IsGenericTypeof(iface, type))
            {
                return type.GetGenericArguments()[0]; // verify it
            }
            return null;
        }

        /// <summary>
        /// Determines whether [is generic typeof] [the specified iface].
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="type">The type.</param>
        /// <returns>
        ///   <c>true</c> if [is generic typeof] [the specified iface]; otherwise, <c>false</c>.
        /// </returns>
        /// <exception cref="System.ApplicationException"></exception>
        public static bool IsGenericTypeof(Type iface, Type type)
        {
            if (iface == null || type == null)
            {
                var ex = new TangApplicationException(string.Format(CultureInfo.CurrentCulture,
                                                             "The type passed in IsGenericTypeof is null: iface : {0} type: {1}. ",
                                                             iface, type));
                Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
            if (type.IsGenericType)
            {
                if (GetAssemblyQualifiedName(iface).Equals(GetAssemblyQualifiedNameForGeneric(type)))
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Classes the and ancestors.
        /// </summary>
        /// <param name="c">The c.</param>
        /// <returns></returns>
        public static IEnumerable<Type> ClassAndAncestors(Type c)
        {
            List<Type> workQueue = new List<Type>();
            workQueue.Add(c); // including itself

            foreach (Type t in c.GetInterfaces())
            {
                workQueue.Add(t);
            }

            Type b = c.BaseType;
            while (b != null)
            {
                workQueue.Add(b);
                b = b.BaseType;
            }

            if (c.IsInterface)
            {
                workQueue.Add(typeof(object));
            }

            return workQueue;
        }

        public static IEnumerable<Type> ClassAndAncestorsExcludeSelf(Type c)
        {
            List<Type> workQueue = new List<Type>();
            //// workQueue.Add(c); // including itself

            foreach (Type t in c.GetInterfaces())
            {
                workQueue.Add(t);
            }

            Type b = c.BaseType;
            while (b != null)
            {
                workQueue.Add(b);
                b = b.BaseType;
            }

            if (c.IsInterface)
            {
                workQueue.Add(typeof(object));
            }

            return workQueue;
        }

        /// <summary>
        /// Boxes the class.
        /// </summary>
        /// <param name="c">The c.</param>
        /// <returns></returns>
        /// <exception cref="System.NotSupportedException">Encountered unknown primitive type!</exception>
        public static Type BoxClass(Type c)
        {
            if (c.IsPrimitive && c != typeof(Type))
            {
                if (c == typeof(bool))
                {
                    return typeof(bool);
                }
                else if (c == typeof(byte))
                {
                    return typeof(byte);
                }
                else if (c == typeof(char))
                {
                    return typeof(char);
                }
                else if (c == typeof(short))
                {
                    return typeof(short);
                }
                else if (c == typeof(int))
                {
                    return typeof(int);
                }
                else if (c == typeof(long))
                {
                    return typeof(long);
                }
                else if (c == typeof(float))
                {
                    return typeof(float);
                }
                else if (c == typeof(double))
                {
                    return typeof(double);
                }
                else
                {
                    Utilities.Diagnostics.Exceptions.Throw(new NotSupportedException(
                        "Encountered unknown primitive type!"), LOGGER);
                    return c;
                }
            }
            else
            {
                return c;
            }
        }

        /// <summary>
        /// Determines whether the specified to is coercable.
        //  public static boolean isCoercable(Class<?> to, Class<?> from) castable from to both are numbers and from has a few bits or subclass relationship
        /// </summary>
        /// <param name="to">To.</param>
        /// <param name="from">From.</param>
        /// <returns>
        ///   <c>true</c> if the specified to is coercable; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsCoercable(Type to, Type from)
        {
            to = BoxClass(to);
            from = BoxClass(from);
            ////TODO
            ////if (Number.class.isAssignableFrom(to)
            ////   && Number.class.isAssignableFrom(from)) {
            ////return sizeof.get(from) <= sizeof.get(to);
            return to.IsAssignableFrom(from);
            //// return IsAssignableFromIgnoreGeneric(to, from);
        }

        /// <summary>
        /// Determines whether [is assignable from ignore generic] [the specified to].
        /// </summary>
        /// <param name="to">To.</param>
        /// <param name="from">From.</param>
        /// <returns>
        ///   <c>true</c> if [is assignable from ignore generic] [the specified to]; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsAssignableFromIgnoreGeneric(Type to, Type from)
        {
            var f = ClassAndAncestors(from);
            foreach (Type t in f)
            {
                if (GetAssemblyQualifiedName(to).Equals(GetAssemblyQualifiedNameForGeneric(t)))
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Ensures the type of the interface. For generic types, full name could be null. In this case, we need to 
        /// get GetGenericTypeDefinition for the type so that to retain all teh type information
        /// </summary>
        /// <param name="interf">The interf.</param>
        /// <returns></returns>
        public static Type EnsureInterfaceType(Type interf)
        {
            if (interf != null && interf.IsGenericType && null == interf.FullName)
            {
                return interf.GetGenericTypeDefinition(); // this is to test if this line is ever reached
            }
            return interf;
        }

        /// <summary>
        /// Gets the assembly qualified name for generic.
        /// </summary>
        /// <param name="t">The t.</param>
        /// <returns></returns>
        public static string GetAssemblyQualifiedNameForGeneric(Type t)
        {
            Type t1 = t;
            if (t.IsGenericType)
            {
                t1 = t.GetGenericTypeDefinition();
            }
            return t1.AssemblyQualifiedName;
        }

        /// <summary>
        /// Determines whether [is instance of generic] [the specified p].
        /// </summary>
        /// <param name="p">The p.</param>
        /// <param name="t">The t.</param>
        /// <returns>
        ///   <c>true</c> if [is instance of generic] [the specified p]; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsInstanceOfGeneric(object p, Type t)
        {
            foreach (var g in ReflectionUtilities.ClassAndAncestors(p.GetType()))
            {
                if (GetAssemblyQualifiedNameForGeneric(t).Equals(GetAssemblyQualifiedNameForGeneric(g)))
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Gets the name of the type by.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns></returns>
        /// <exception cref="System.ApplicationException">Not able to get Type from the name provided:  +  name</exception>
        public static Type GetTypeByName(string name)
        {
            Type t = null;

            t = Type.GetType(name);

            if (t == null)
            {
                foreach (var a in AppDomain.CurrentDomain.GetAssemblies())
                {
                    t = a.GetType(name);
                    if (t != null)
                    {
                        break;
                    }
                }
            }
            if (t == null)
            {
                Utilities.Diagnostics.Exceptions.Throw(
                    new TangApplicationException("Not able to get Type from the name provided: " + name), LOGGER);
            }

            return t;
        }

        /// <summary>
        /// Gets the enclosing classes.
        /// </summary>
        /// <param name="t">The t.</param>
        /// <returns></returns>
        public static Type[] GetEnclosingClasses(Type t)
        {
            IList<Type> l = new List<Type>();
            l.Add(t);
            Type current = t.DeclaringType;
            while (current != null)
            {
                l.Add(current);
                current = current.DeclaringType;
            }
            return l.Reverse().ToArray();
        }

        /// <summary>
        /// Gets the enclosing class names.
        /// </summary>
        /// <param name="t">The t.</param>
        /// <returns></returns>
        /// <exception cref="System.ApplicationException">The Type passed to GetEnclosingClassShortNames is null</exception>
        public static string[] GetEnclosingClassNames(Type t)
        {
            if (t == null)
            {
                Utilities.Diagnostics.Exceptions.Throw(new TangApplicationException("The Type passed to GetEnclosingClassShortNames is null"), LOGGER);
            }
            Type[] ts = GetEnclosingClasses(t);
            string[] result = new string[ts.Length];
            for (int i = 0; i < ts.Length; i++)
            {
                result[i] = GetAssemblyQualifiedName(ts[i]);
            }

            return result;
        }

        /// <summary>
        /// Gets the enclosing class names.
        /// </summary>
        /// <param name="fullName">The full name.</param>
        /// <returns></returns>
        /// <exception cref="System.ApplicationException">The name passed to GetEnclosingClassShortNames is null</exception>
        public static string[] GetEnclosingClassNames(string fullName)
        {
            if (fullName == null)
            {
                Utilities.Diagnostics.Exceptions.Throw(new TangApplicationException("The name passed to GetEnclosingClassShortNames is null"), LOGGER);
            }
            Type t = ReflectionUtilities.GetTypeByName(fullName);
            return GetEnclosingClassNames(t);
        }

        /// <summary>
        /// Gets the named parameter target or null.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        /// <exception cref="ClassHierarchyException">Named parameter  + GetName(type) +  implements 
        /// + multiple interfaces.  It is only allowed to implement Name</exception>
        public static Type GetNamedParameterTargetOrNull(Type type)
        {
            var npAnnotation = type.GetCustomAttribute<NamedParameterAttribute>();
            if (npAnnotation != null)
            {
                Type[] intfs = type.GetInterfaces();
                if (intfs.Length > 1)
                {
                    var ex = new ClassHierarchyException("Named parameter " + GetName(type) + " implements "
                                  + "multiple interfaces.  It is only allowed to implement Name<T>");
                    Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
                else if (intfs.Length == 0 || !IsName(intfs[0]))
                {
                    var ex = new ClassHierarchyException("Found illegal [NamedParameter " + GetName(type)
                                  + " does not implement Name<T>");
                    Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
                Type[] args = intfs[0].GetGenericArguments();
                if (args.Length > 1)
                {
                    var ex = new ClassHierarchyException("Found illegal [NamedParameter " + GetName(type)
                        + " that has more than one arguments");
                    Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
                if (args.Length == 0)
                {
                    var ex = new ClassHierarchyException("Found illegal [NamedParameter " + GetName(type)
                        + " that has no argument");
                    Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
                if (HasConstructor(type) || HasInjectableConstructor(type))
                {
                    var ex = new ClassHierarchyException("Named parameter " + GetName(type) + " has "
                                  + (HasInjectableConstructor(type) ? "an injectable" : "a") + " constructor. "
                                  + " Named parameters must not declare any constructors.");
                    Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }

                return args[0];
            }

            if (ImplementName(type))
            {
                // Implement Name<> but no  [NamedParameter] attribute
                var ex = new ClassHierarchyException("Named parameter " + GetName(type)
                                  + " is missing its [NamedParameter] attribute.");
                Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
            return null;
        }

        public static IEnumerable<Type> GetInterfaces(Type type, bool includeInherited)
        {
            if (includeInherited || type.BaseType == null)
            {
                return type.GetInterfaces();
            }
            else
            {
                return type.GetInterfaces().Except(type.BaseType.GetInterfaces());
            }
        }

        // Here is a more elaborate hack to test for anonymous type:
        // http://stackoverflow.com/questions/2483023/how-to-test-if-a-type-is-anonymous
        // compiler generated classes are always recreatable and need not additional references to check for.
        public static bool IsAnonymousType(Type type)
        {
            if (type != null)
            {
                // HACK: The only way to detect anonymous types right now.
                CompareInfo myComp = CultureInfo.CurrentCulture.CompareInfo;
                return CustomAttributeExtensions.IsDefined(type, typeof(CompilerGeneratedAttribute), false)
                       && type.IsGenericType && type.Name.Contains("AnonymousType")
                       && (myComp.IsPrefix(type.Name, "<>", CompareOptions.IgnoreCase) || myComp.IsPrefix(type.Name, "VB$", CompareOptions.IgnoreCase))
                       && (type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;
            }
            return false;
        }

        private static bool ImplementName(Type type)
        {
            foreach (Type t in type.GetInterfaces())
            {
                if (IsName(t))
                {
                    return true;
                }
            }
            return false;
        }

        private static bool IsName(Type t)
        {
            if (t.IsGenericType)
            {
                return t.GetGenericTypeDefinition().AssemblyQualifiedName.Equals(typeof(Name<>).AssemblyQualifiedName);
            }
            return false;
        }

        private static bool HasConstructor(Type type)
        {
            bool hasConstructor = false;

            ConstructorInfo[] constructors =
                type.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);

            if (constructors.Length > 1)
            {
                hasConstructor = true;
            }

            if (constructors.Length == 1)
            {
                ConstructorInfo c = constructors[0];
                ParameterInfo[] p = c.GetParameters();
                if (p.Length > 1)
                {
                    // Multiple args. Definitely not implicit.
                    hasConstructor = true;
                }
                else if (p.Length == 1)
                {
                    // One arg. Could be an inner class, in which case the compiler
                    // included an implicit one parameter constructor that takes the
                    // enclosing type.
                    if (p[0].ParameterType != type.DeclaringType)
                    {
                        hasConstructor = true;
                    }
                }
            }
            return hasConstructor;
        }

        private static bool HasInjectableConstructor(Type type)
        {
            bool isInjectable = false;

            ConstructorInfo[] constructors =
                type.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);

            foreach (ConstructorInfo c in constructors)
            {
                foreach (Attribute a in c.GetCustomAttributes())
                {
                    if (a is InjectAttribute)
                    {
                        isInjectable = true;
                    }
                }
            }
            return isInjectable;
        }
    }
}