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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Examples.Tasks.HelloTask;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Examples;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.ClassHierarchy
{
    public class TestClassHierarchy
    {
        public IClassHierarchy ns = null;

        public TestClassHierarchy()
        {
            if (ns == null)
            {
                TangImpl.Reset();
                ns = TangFactory.GetTang().GetClassHierarchy(new string[] { FileNames.Examples, FileNames.Common, FileNames.Tasks });
            }
        }

        [Fact]
        public void TestString()
        {
            INode n = null;
            try
            {
                n = ns.GetNode("System");
            }
            catch (ApplicationException)
            {
            }
            catch (NameResolutionException)
            {
            }
            Assert.Null(n);

            Assert.NotNull(ns.GetNode(typeof(string).AssemblyQualifiedName));

            string msg = null;  
            try
            {
                ns.GetNode("org.apache");
                msg = "Didn't get expected exception";
            }
            catch (ApplicationException)
            {
            }
            catch (NameResolutionException)
            {
            }
            Assert.True(msg == null, msg);
        }

        [Fact]
        public void TestInt()
        {
            INode n = null;
            try
            {
                n = ns.GetNode("System");
            }
            catch (ApplicationException)
            {
            }
            catch (NameResolutionException)
            {
            }
            Assert.Null(n);

            Assert.NotNull(ns.GetNode(typeof(int).AssemblyQualifiedName));

            string msg = null;      
            try
            {
                ns.GetNode("org.apache");
                msg = "Didn't get expected exception";
            }
            catch (ApplicationException)
            {
            }
            catch (NameResolutionException)
            {
            }
            Assert.True(msg == null, msg);        
        }

        [Fact]
        public void TestSimpleConstructors()
        {
            IClassNode cls = (IClassNode)ns.GetNode(typeof(SimpleConstructors).AssemblyQualifiedName);
            Assert.True(cls.GetChildren().Count == 0);
            IList<IConstructorDef> def = cls.GetInjectableConstructors();
            Assert.Equal(3, def.Count);
        }

        [Fact]
        public void TestTimer()
        {
            IClassNode timerClassNode = (IClassNode)ns.GetNode(typeof(Timer).AssemblyQualifiedName);
            INode secondNode = ns.GetNode(typeof(Timer.Seconds).AssemblyQualifiedName);
            Assert.Equal(secondNode.GetFullName(), ReflectionUtilities.GetAssemblyQualifiedName(typeof(Timer.Seconds)));
        }

        [Fact]
        public void TestNamedParameterConstructors()
        {
            var node = ns.GetNode(typeof(NamedParameterConstructors).AssemblyQualifiedName);
            Assert.Equal(node.GetFullName(), ReflectionUtilities.GetAssemblyQualifiedName(typeof(NamedParameterConstructors)));
        }

        [Fact]
        public void TestArray()
        {
            Type t = (new string[0]).GetType();
            INode node = ns.GetNode(t.AssemblyQualifiedName);
            Assert.Equal(node.GetFullName(), t.AssemblyQualifiedName);
        }

        [Fact]
        public void TestRepeatConstructorArg()
        {
            TestNegativeCase(typeof(RepeatConstructorArg),
                "Repeated constructor parameter detected.  Cannot inject constructor RepeatConstructorArg(int,int).");
        }

        [Fact]
        public void TestRepeatConstructorArgClasses()
        {
            TestNegativeCase(typeof(RepeatConstructorArgClasses),
                "Repeated constructor parameter detected.  Cannot inject constructor RepeatConstructorArgClasses(A, A).");
        }

        [Fact]
        public void testLeafRepeatedConstructorArgClasses()
        {
            INode node = ns.GetNode(typeof(LeafRepeatedConstructorArgClasses).AssemblyQualifiedName);
            Assert.Equal(node.GetFullName(), typeof(LeafRepeatedConstructorArgClasses).AssemblyQualifiedName);
        }

        [Fact]
        public void TestNamedRepeatConstructorArgClasses()
        {
            INode node = ns.GetNode(typeof(NamedRepeatConstructorArgClasses).AssemblyQualifiedName);
            Assert.Equal(node.GetFullName(), typeof(NamedRepeatConstructorArgClasses).AssemblyQualifiedName);
        }

        [Fact]
        public void TestResolveDependencies() 
        {
            ns.GetNode(typeof(SimpleConstructors).AssemblyQualifiedName);
            Assert.NotNull(ns.GetNode(typeof(string).AssemblyQualifiedName));
        }

        [Fact]
        public void TestDocumentedLocalNamedParameter()
        {
            var node = ns.GetNode(typeof(DocumentedLocalNamedParameter).AssemblyQualifiedName);
            Assert.Equal(node.GetFullName(), ReflectionUtilities.GetAssemblyQualifiedName(typeof(DocumentedLocalNamedParameter)));
        }

        [Fact]
        public void TestNamedParameterTypeMismatch()
        {
            TestNegativeCase(typeof(NamedParameterTypeMismatch),
                "Named parameter type mismatch in NamedParameterTypeMismatch. Constructor expects a System.String but Foo is a System.Int32.");
        }

        [Fact]
        public void TestUnannotatedName()
        {
            TestNegativeCase(typeof(UnannotatedName),
                "Named parameter UnannotatedName is missing its [NamedParameter] attribute.");
        }

        [Fact]
        public void TestAnnotatedNotName()
        {
            TestNegativeCase(typeof(AnnotatedNotName),
                "Found illegal [NamedParameter] AnnotatedNotName does not implement Name<T>.");
        }

        [Fact]
        public void TestAnnotatedNameWrongInterface()
        {
            TestNegativeCase(typeof(AnnotatedNameWrongInterface),
                "Found illegal [NamedParameter] AnnotatedNameWrongInterface does not implement Name<T>.");
        }

        [Fact]
        public void TestAnnotatedNameMultipleInterfaces()
        {
            TestNegativeCase(typeof(AnnotatedNameMultipleInterfaces),
                "Named parameter Org.Apache.REEF.Tang.Implementation.AnnotatedNameMultipleInterfaces implements multiple interfaces.  It is only allowed to implement Name<T>.");
        }

        [Fact]
        public void TestUnAnnotatedNameMultipleInterfaces()
        {
            TestNegativeCase(typeof(UnAnnotatedNameMultipleInterfaces),
                "Named parameter Org.Apache.REEF.Tang.Implementation.UnAnnotatedNameMultipleInterfaces is missing its @NamedParameter annotation.");
        }

        [Fact]
        public void TestNameWithConstructor()
        {
            TestNegativeCase(typeof(NameWithConstructor),
                "Named parameter Org.Apache.REEF.Tang.Implementation.NameWithConstructor has a constructor.  Named parameters must not declare any constructors.");
        }

        [Fact]
        public void TestNameWithZeroArgInject()
        {
            TestNegativeCase(typeof(NameWithZeroArgInject),
                "Named parameter Org.Apache.REEF.Tang.Implementation.NameWithZeroArgInject has an injectable constructor.  Named parameters must not declare any constructors.");
        }

        [Fact]
        public void TestInjectNonStaticLocalArgClass()
        {
            var node = ns.GetNode(typeof(InjectNonStaticLocalArgClass).AssemblyQualifiedName);
            Assert.Equal(node.GetFullName(), typeof(InjectNonStaticLocalArgClass).AssemblyQualifiedName);
        }

        [Fact]
        public void TestInjectNonStaticLocalType()
        {
            var node = ns.GetNode(typeof(InjectNonStaticLocalType).AssemblyQualifiedName);
            Assert.Equal(node.GetFullName(), typeof(InjectNonStaticLocalType).AssemblyQualifiedName);
        }

        [Fact]
        public void TestOKShortNames()
        {
            var node = ns.GetNode(typeof(ShortNameFooA).AssemblyQualifiedName);
            Assert.Equal(node.GetFullName(), typeof(ShortNameFooA).AssemblyQualifiedName);
        }

        public void TestConflictingShortNames()
        {
            string msg = null;
            try
            {
                var nodeA = ns.GetNode(typeof(ShortNameFooA).AssemblyQualifiedName);
                var nodeB = ns.GetNode(typeof(ShortNameFooB).AssemblyQualifiedName);
                msg = 
                    "ShortNameFooA and ShortNameFooB have the same short name" +
                    nodeA.GetName() + nodeB.GetName();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            Assert.True(msg == null, msg);
        }

        [Fact]
        public void TestRoundTripInnerClassNames()
        {
            INode node = ns.GetNode(typeof(Nested.Inner).AssemblyQualifiedName);
            Assert.Equal(node.GetFullName(), typeof(Nested.Inner).AssemblyQualifiedName);
        }

        [Fact]
        public void TestRoundTripAnonInnerClassNames()
        {
            INode node1 = ns.GetNode(typeof(AnonNested.X1).AssemblyQualifiedName);
            INode node2 = ns.GetNode(typeof(AnonNested.Y1).AssemblyQualifiedName);
            Assert.NotEqual(node1.GetFullName(), node2.GetFullName());

            Type t1 = ReflectionUtilities.GetTypeByName(node1.GetFullName());
            Type t2 = ReflectionUtilities.GetTypeByName(node2.GetFullName());

            Assert.NotSame(t1, t2);
        }

        [Fact]
        public void TestNameCantBindWrongSubclassAsDefault()
        {
            TestNegativeCase(typeof(BadName),
            "class org.apache.reef.tang.implementation.BadName defines a default class Int32 with a type that does not extend of its target's type string");
        }

        [Fact]
        public void TestNameCantBindWrongSubclassOfArgumentAsDefault()
        {
            TestNegativeCase(typeof(BadNameForGeneric),
                        "class BadNameForGeneric defines a default class Int32 with a type that does not extend of its target's string in ISet<string>");
        }

        [Fact]
        public void TestNameCantBindSubclassOfArgumentAsDefault()
        {
            ns = TangFactory.GetTang().GetClassHierarchy(new string[] { FileNames.Examples, FileNames.Common, FileNames.Tasks });
            INode node = ns.GetNode(typeof(GoodNameForGeneric).AssemblyQualifiedName);
            Assert.Equal(node.GetFullName(), typeof(GoodNameForGeneric).AssemblyQualifiedName);
        }

        [Fact]
        public void TestInterfaceCantBindWrongImplAsDefault()
        {
            TestNegativeCase(typeof(IBadIfaceDefault),
                             "interface IBadIfaceDefault declares its default implementation to be non-subclass class string");
        }

        private void TestNegativeCase(Type clazz, string message)
        {
            string msg = null;
            try
            {
                var node = ns.GetNode(typeof(IBadIfaceDefault).AssemblyQualifiedName);
                msg = message + node.GetName();
            }
            catch (Exception)
            {
            }
            Assert.True(msg == null, msg);
        }

        [Fact]
        public void TestParseableDefaultClassNotOK()
        {
            TestNegativeCase(typeof(BadParsableDefaultClass),
                 "Named parameter BadParsableDefaultClass defines default implementation for parsable type System.string");
        }

        [Fact]
        public void testGenericTorture1()
        {
            g(typeof(GenericTorture1));
        }
        [Fact]
        public void testGenericTorture2()
        {
            g(typeof(GenericTorture2));
        }
        [Fact]
        public void testGenericTorture3()
        {
            g(typeof(GenericTorture3));
        }
        [Fact]
        public void testGenericTorture4()
        {
            g(typeof(GenericTorture4));
        }

        public string s(Type t)
        {
            return ReflectionUtilities.GetAssemblyQualifiedName(t);
        }
        public INode g(Type t)
        {
            INode n = ns.GetNode(s(t)); 
            Assert.NotNull(n);
            return n;
        }

        [Fact]
        public void TestHelloTaskNode()
        {
            var node = ns.GetNode(typeof(HelloTask).AssemblyQualifiedName);
            Assert.Equal(node.GetFullName(), ReflectionUtilities.GetAssemblyQualifiedName(typeof(HelloTask)));
        }

        [Fact]
        public void TestITackNode()
        {
            var node = ns.GetNode(typeof(ITask).AssemblyQualifiedName);
            Assert.Equal(node.GetFullName(), ReflectionUtilities.GetAssemblyQualifiedName(typeof(ITask)));
        }

        [Fact]
        public void TestNamedParameterIdentifier()
        {
            var node = ns.GetNode(typeof(TaskConfigurationOptions.Identifier).AssemblyQualifiedName);
            Assert.Equal(node.GetFullName(), ReflectionUtilities.GetAssemblyQualifiedName(typeof(TaskConfigurationOptions.Identifier)));
        }
        [Fact]
        public void TestInterface()
        {
            g(typeof(A));
            g(typeof(MyInterface<int>));
            g(typeof(MyInterface<string>));
            g(typeof(B));

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder();

            cb.BindImplementation(GenericType<MyInterface<string>>.Class, GenericType<MyImplString>.Class);
            cb.BindImplementation(GenericType<MyInterface<int>>.Class, GenericType<MyImplInt>.Class);
            IConfiguration conf = cb.Build();
            IInjector i = tang.NewInjector(conf);

            var a = (A)i.GetInstance(typeof(A));
            var implString = (MyImplString)i.GetInstance(typeof(MyImplString));
            var implInt = (MyImplString)i.GetInstance(typeof(MyImplString));
            var b = (B)i.GetInstance(typeof(B));
            var c = (C)i.GetInstance(typeof(C));

            Assert.NotNull(a);
            Assert.NotNull(implString);
            Assert.NotNull(implInt);
            Assert.NotNull(b);
            Assert.NotNull(c);
        }
    }

    [NamedParameter]
    class GenericTorture1 : Name<ISet<string>> 
    {
    }
    [NamedParameter]
    class GenericTorture2 : Name<ISet<ISet<string>>>
    {
    }
    [NamedParameter]
    class GenericTorture3 : Name<IDictionary<ISet<string>, ISet<string>>>
    {
    }
    [NamedParameter]
    class GenericTorture4 : Name<IDictionary<string, string>>
    {
    }

    public interface MyInterface<T>
    {
    }

    public class RepeatConstructorArg
    {
        [Inject]
        public RepeatConstructorArg(int x, int y)
        {
        }
    }

    public class RepeatConstructorArgClasses
    {
        [Inject]
        public RepeatConstructorArgClasses(A x, A y)
        {
        }
    }

    public class A : MyInterface<int>, MyInterface<string>
    {
        [Inject]
        A()
        {
        }
    }

    public class MyImplString : MyInterface<string>
    {
        [Inject]
        public MyImplString()
        {
        }
    }

    public class B
    {
        [Inject]
        public B(MyInterface<string> b)
        {
        }
    }

    public class MyImplInt : MyInterface<int>
    {
        [Inject]
        public MyImplInt()
        {
        }
    }
    public class C
    {
        [Inject]
        public C(MyInterface<int> b)
        {
        }
    }
    public class LeafRepeatedConstructorArgClasses
    {
        public static class A
        {
            public class AA
            {
            }
        }

        public static class B
        {
            public class AA
            {
            }
        }

        public class C
        {
            [Inject]
            public C(A.AA a, B.AA b)
            {
            }
        }
    }

    class D
    {        
    }
    [NamedParameter]
    class D1 : Name<D> 
    {
    }

    [NamedParameter]
    class D2 : Name<D> 
    {
    }

    class NamedRepeatConstructorArgClasses 
    {
        [Inject]
        public NamedRepeatConstructorArgClasses([Parameter(typeof(D1))] D x, [Parameter(typeof(D2))] D y) 
        {
        }
    }

    class NamedParameterTypeMismatch 
    {
        [NamedParameter(Documentation = "doc.stuff", DefaultValue = "1")]
        class Foo : Name<int> 
        {
        }

        [Inject]
        public NamedParameterTypeMismatch([Parameter(Value = typeof(Foo))] string s) 
        {
        }
    }

    class UnannotatedName : Name<string> 
    {
    }

    interface I1 
    {
    }

    [NamedParameter(Documentation = "c")]
    class AnnotatedNotName 
    {
    }

    [NamedParameter(Documentation = "c")]
    class AnnotatedNameWrongInterface : I1 
    {
    }

    class UnAnnotatedNameMultipleInterfaces : Name<object>, I1 
    {
    }

    [NamedParameter(Documentation = "c")]
    class AnnotatedNameMultipleInterfaces : Name<object>, I1 
    {
    }

    [NamedParameter(Documentation = "c")]
    class NameWithConstructor : Name<object> 
    {
        private NameWithConstructor(int i) 
        {
        }
    }

    [NamedParameter]
    class NameWithZeroArgInject : Name<object> 
    {
        [Inject]
        public NameWithZeroArgInject() 
        {
        }
    }

    class InjectNonStaticLocalArgClass
    {
        class NonStaticLocal
        {
        }

        [Inject]
        InjectNonStaticLocalArgClass(NonStaticLocal x)
        {
        }
    }

    class InjectNonStaticLocalType
    {
        class NonStaticLocal
        {
            [Inject]
            NonStaticLocal(NonStaticLocal x)
            {
            }
        }
    }

    [NamedParameter(ShortName = "foo")]
    public class ShortNameFooA : Name<string>
    {
    }

    // when same short name is used, exception would throw when building the class hierarchy
    [NamedParameter(ShortName = "foo")]
    public class ShortNameFooB : Name<int>
    {
    }

    class Nested
    {
        public class Inner
        {
        }
    }

    class AnonNested 
    {
        public interface X 
        {
        }

        public class X1 : X
        {
            // int i;
        }

        public class Y1 : X
        {
            // int j;
        }

        public static X XObj = new X1();
        public static X YObj = new Y1();
    }

    // Negative case: Int32 doesn't match string
    [NamedParameter(DefaultClass = typeof(Int32))]
    class BadName : Name<string>
    {        
    }

    // Negative case: Int32 doesn't match string in the ISet
    [NamedParameter(DefaultClass = typeof(Int32))]
    class BadNameForGeneric : Name<ISet<string>>
    {
    }

    // Positive case: type matched. ISet is not in parsable list
    [NamedParameter(DefaultClass = typeof(string))]
    class GoodNameForGeneric : Name<ISet<string>>
    {
    }

    [DefaultImplementation(typeof(string))]
    interface IBadIfaceDefault
    {        
    }

    // negative case: type matched. However, string is in the parsable list and DefaultClass is not null. 
    [NamedParameter(DefaultClass = typeof(string))]
    class BadParsableDefaultClass : Name<string>
    {        
    }
 }