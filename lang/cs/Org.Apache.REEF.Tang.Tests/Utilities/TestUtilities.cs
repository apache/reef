/**
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

using System;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Tang.Examples;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Tests.Utilities
{
    [TestClass]
    public class TestUtilities
    {
        [TestMethod]
        public void TestIsAssignableFromIgnoreGeneric()
        {
            var result = ReflectionUtilities.IsAssignableFromIgnoreGeneric(typeof(IExternalConstructor<>), typeof(Foo));
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void TestIsAssignableFromIgnoreGenericNegtive()
        {
            var result = ReflectionUtilities.IsAssignableFromIgnoreGeneric(typeof(ISet<>), typeof(Foo));
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void TestIsInstanceOfGeneric()
        {
            var p1 = new OptionalParameter<int>();
            bool r1 = ReflectionUtilities.IsInstanceOfGeneric(p1, typeof(OptionalParameter<>));
            var p2 = new RequiredParameter<string>();
            bool r2 = ReflectionUtilities.IsInstanceOfGeneric(p2, typeof(RequiredParameter<>));
            Assert.IsTrue(r1);
            Assert.IsTrue(r2);
        }

        [TestMethod]
        public void TestGetInterfaceTargetForTypeInheritFromGeneric()
        {
            Type result = ReflectionUtilities.GetInterfaceTarget(typeof(IExternalConstructor<>), typeof(Foo));
            Assert.AreEqual(result, typeof(Int32));
        }

        class Foo : IExternalConstructor<Int32>
        {
            public Int32 NewInstance()
            {
                return 3;
            }
        }

        [TestMethod]
        public void TestGetEnclosingClassShortNameByType()
        {
            var asm = Assembly.Load(FileNames.Examples);
            Type seconds = asm.GetType(FileNames.Seconds);
            Type timer = asm.GetType(FileNames.Timer);

            string[] pathSeconds = ReflectionUtilities.GetEnclosingClassNames(seconds);
            Assert.AreEqual(pathSeconds[0], timer.AssemblyQualifiedName);
            Assert.AreEqual(pathSeconds[1], seconds.AssemblyQualifiedName);

            string[] pathTime = ReflectionUtilities.GetEnclosingClassNames(timer);
            Assert.AreEqual(pathTime[0], timer.AssemblyQualifiedName);
        }

        [TestMethod]
        public void TestGetName()
        {
            var asm = Assembly.Load(FileNames.Examples);
            Type B2 = asm.GetType(FileNames.B2);

            string n = ReflectionUtilities.GetName(B2);
            Assert.AreEqual(n, B2.FullName);
        }

        [TestMethod]
        public void TestGetEnclosingTypesInclusive()
        {
            var asm = Assembly.Load(FileNames.Examples);
            Type B2 = asm.GetType(FileNames.B2);
            Type[] ts = ReflectionUtilities.GetEnclosingClasses(B2);
            Assert.AreEqual(ts[0], asm.GetType(FileNames.B));
            Assert.AreEqual(ts[1], asm.GetType(FileNames.B1));
            Assert.AreEqual(ts[2], asm.GetType(FileNames.B2));
        }

        [TestMethod]
        public void TestGetEnclosingClassShortNameByName()
        {
            var asm = Assembly.Load(FileNames.Examples);
            Type b = asm.GetType(FileNames.B);
            Type b1 = asm.GetType(FileNames.B1);
            Type b2 = asm.GetType(FileNames.B2);

            string[] path = ReflectionUtilities.GetEnclosingClassNames(FileNames.B2);
            Assert.AreEqual(path[0], b.AssemblyQualifiedName);
            Assert.AreEqual(path[1], b1.AssemblyQualifiedName);
            Assert.AreEqual(path[2], b2.AssemblyQualifiedName);
        }

        [TestMethod]
        public void TestGetEnclosingClassShortNameByFullName()
        {
            var asm = Assembly.Load(FileNames.Examples);
            Type c = asm.GetType(FileNames.B2);
            Type b = asm.GetType(FileNames.B);
            Type b1 = asm.GetType(FileNames.B1);
            Type b2 = asm.GetType(FileNames.B2);
            string[] path = ReflectionUtilities.GetEnclosingClassNames(c.FullName);
            Assert.AreEqual(path[0], b.AssemblyQualifiedName);
            Assert.AreEqual(path[1], b1.AssemblyQualifiedName);
            Assert.AreEqual(path[2], b2.AssemblyQualifiedName);
        }

        [TestMethod]
        public void TestGetEnclosingClassShortNameByAssemblyQualifiedName()
        {
            var asm = Assembly.Load(FileNames.Examples);
            Type c = asm.GetType(FileNames.B2);
            Type b = asm.GetType(FileNames.B);
            Type b1 = asm.GetType(FileNames.B1);
            Type b2 = asm.GetType(FileNames.B2);

            string[] path = ReflectionUtilities.GetEnclosingClassNames(c.AssemblyQualifiedName);

            Assert.AreEqual(path[0], b.AssemblyQualifiedName);
            Assert.AreEqual(path[1], b1.AssemblyQualifiedName);
            Assert.AreEqual(path[2], b2.AssemblyQualifiedName);
        }
        
        [TestMethod]
        public void AssemblyNamesTest()
        {
            var asm = Assembly.Load(FileNames.Examples);
            Type seconds = asm.GetType(FileNames.Seconds);

            var name = seconds.Name;
            var fullName = seconds.FullName;
            var assemblyName = seconds.AssemblyQualifiedName;

            Type[] i = seconds.GetInterfaces();

            foreach (Type t in i)
            {
                var name1 = t.Name;
                var fullName1 = t.FullName;
                var assemblyName1 = t.AssemblyQualifiedName;
            }
        }

        [TestMethod]
        public void TestGetInterfaceTargetForGenericType()
        {
            Type iface = typeof(ISet<>);
            Type type = typeof(MySet<string>);
            Type p = ReflectionUtilities.GetInterfaceTarget(iface, type);
            Assert.IsTrue(p.Equals(typeof(string)));
        }

        [TestMethod]
        public void TestGetInterfaceTargetForSystemGenericType()
        {
            Type iface = typeof(ISet<>);
            Type type = typeof(ISet<int>);
            Type p = ReflectionUtilities.GetInterfaceTarget(iface, type);
            Assert.IsTrue(p.Equals(typeof(int)));
        }
    }

    public class MySet<T> : ISet<T>
    {
        public bool Add(T item)
        {
            throw new NotImplementedException();
        }

        public void ExceptWith(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public void IntersectWith(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool IsProperSubsetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool IsProperSupersetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool IsSubsetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool IsSupersetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool Overlaps(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool SetEquals(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public void SymmetricExceptWith(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public void UnionWith(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        void ICollection<T>.Add(T item)
        {
            throw new NotImplementedException();
        }

        public void Clear()
        {
            throw new NotImplementedException();
        }

        public bool Contains(T item)
        {
            throw new NotImplementedException();
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }

        public int Count
        {
            get { throw new NotImplementedException(); }
        }

        public bool IsReadOnly
        {
            get { throw new NotImplementedException(); }
        }

        public bool Remove(T item)
        {
            throw new NotImplementedException();
        }

        public IEnumerator<T> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }
}