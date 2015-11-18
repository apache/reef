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
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Examples;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.InjectionPlan;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Tests.Tang
{
    [TestClass]
    public class TestTang
    {
        private static ITang tang;

        private static Assembly asm = null;

        [ClassInitialize]
        public static void ClassSetup(TestContext context)
        {
            asm = Assembly.Load(FileNames.Examples);
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
        }

        [TestInitialize()]
        public void TestSetup()
        {
            MustBeSingleton.alreadyInstantiated = false;
            tang = TangFactory.GetTang();
        }

        [TestCleanup()]
        public void TestCleanup()
        {
        }

        [TestMethod]
        public void TestSingleton()
        {
            IInjector injector = tang.NewInjector();
            Assert.IsNotNull(injector.GetInstance(typeof (TwoSingletons)));
            Assert.IsNotNull(injector.GetInstance(typeof (TwoSingletons)));
        }

        [TestMethod]
        public void TestNotSingleton()
        {
            TwoSingletons obj = null;
            Assert.IsNotNull(tang.NewInjector().GetInstance(typeof (TwoSingletons)));
            try
            {
                obj = (TwoSingletons) tang.NewInjector().GetInstance(typeof (TwoSingletons));
            }
            catch (InjectionException)
            {

            }
            Assert.IsNull(obj);
        }

        [TestMethod]
        public void TestRepeatedAmbiguousArgs()
        {
            INode node = null;
            try
            {
                ICsConfigurationBuilder t = tang.NewConfigurationBuilder();
                node =
                    t.GetClassHierarchy()
                     .GetNode(ReflectionUtilities.GetAssemblyQualifiedName(typeof (RepeatedAmbiguousArgs)));
            }
            catch (ClassHierarchyException)
            {
            }
            Assert.IsNull(node);
        }

        [TestMethod]
        public void TestRepeatedOKArgs()
        {
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder();
            cb.BindNamedParameter<RepeatedNamedArgs.A, Int32>(GenericType<RepeatedNamedArgs.A>.Class, "1");
            cb.BindNamedParameter<RepeatedNamedArgs.B, Int32>(GenericType<RepeatedNamedArgs.B>.Class, "2");

            IInjector injector = tang.NewInjector(cb.Build());
            injector.GetInstance(typeof (RepeatedNamedArgs));
        }

        // NamedParameter A has no default_value, so this should throw.
        [TestMethod]
        public void TestOneNamedFailArgs()
        {
            string msg = null;
            try
            {
                tang.NewInjector().GetInstance<OneNamedSingletonArgs>();
                msg =
                    "Cannot inject OneNamedSingletonArgs: cOneNamedSingletonArgs missing argument OneNamedSingletonArgs+A";
            }
            catch (Exception)
            {
            }
            Assert.IsNull(msg);
        }

        // NamedParameter A get's bound to a volatile, so this should succeed.
        [TestMethod]
        public void TestOneNamedSingletonOKArgs()
        {
            IInjector i = tang.NewInjector();
            i.BindVolatileParameter(GenericType<OneNamedSingletonArgs.A>.Class, i.GetInstance<MustBeSingleton>());
            OneNamedSingletonArgs o = i.GetInstance<OneNamedSingletonArgs>();
            Assert.IsNotNull(o);
        }


        [TestMethod]
        public void TestRepeatedNamedArgs()
        {
            IInjector i = tang.NewInjector();
            i.BindVolatileParameter(GenericType<RepeatedNamedSingletonArgs.A>.Class,
                                    (MustBeSingleton) i.GetInstance(typeof (MustBeSingleton)));
            i.BindVolatileParameter(GenericType<RepeatedNamedSingletonArgs.B>.Class,
                                    (MustBeSingleton) i.GetInstance(typeof (MustBeSingleton)));
            i.GetInstance(typeof (RepeatedNamedSingletonArgs));
        }

        [TestMethod]
        public void testStraightforwardBuild()
        {
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder();
            cb.BindImplementation(GenericType<Interf>.Class, GenericType<Impl>.Class);
            tang.NewInjector(cb.Build()).GetInstance(typeof (Interf));
        }

        [TestMethod]
        public void TestOneNamedStringArgCantRebind()
        {
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder();
            OneNamedStringArg a =
                (OneNamedStringArg) tang.NewInjector(cb.Build()).GetInstance(typeof (OneNamedStringArg));
            Assert.AreEqual("default", a.s);
            cb.BindNamedParameter<OneNamedStringArg.A, string>(GenericType<OneNamedStringArg.A>.Class, "not default");
            IInjector i = tang.NewInjector(cb.Build());
            Assert.AreEqual("not default", ((OneNamedStringArg) i.GetInstance(typeof (OneNamedStringArg))).s);
            string msg = null;
            try
            {
                i.BindVolatileParameter(GenericType<OneNamedStringArg.A>.Class, "volatile");
                msg =
                    "Attempt to re-bind named parameter Org.Apache.REEF.Tang.OneNamedStringArg$A.  Old value was [not default] new value is [volatile]";
            }
            catch (Exception)
            {
            }
            Assert.IsNull(msg);
        }

        [TestMethod]
        public void TestOneNamedStringArgBind()
        {
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder();
            OneNamedStringArg a = tang.NewInjector(cb.Build()).GetInstance<OneNamedStringArg>();
            Assert.AreEqual("default", a.s);
            cb.BindNamedParameter<OneNamedStringArg.A, string>(GenericType<OneNamedStringArg.A>.Class, "not default");
            IInjector i = tang.NewInjector(cb.Build());
            Assert.AreEqual("not default", i.GetInstance<OneNamedStringArg>().s);
        }

        [TestMethod]
        public void TestOneNamedStringArgVolatile()
        {
            OneNamedStringArg a = tang.NewInjector().GetInstance<OneNamedStringArg>();
            Assert.AreEqual("default", a.s);
            IInjector i = tang.NewInjector();
            i.BindVolatileParameter(GenericType<OneNamedStringArg.A>.Class, "volatile");
            Assert.AreEqual("volatile", i.GetInstance<OneNamedStringArg>().s);
        }

        [TestMethod]
        public void TestTwoNamedStringArgsBind()
        {
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder();
            TwoNamedStringArgs a = tang.NewInjector(cb.Build()).GetInstance<TwoNamedStringArgs>();
            Assert.AreEqual("defaultA", a.a);
            Assert.AreEqual("defaultB", a.b);
            cb.BindNamedParameter<TwoNamedStringArgs.A, string>(GenericType<TwoNamedStringArgs.A>.Class, "not defaultA");
            cb.BindNamedParameter<TwoNamedStringArgs.B, string>(GenericType<TwoNamedStringArgs.B>.Class, "not defaultB");
            IInjector i = tang.NewInjector(cb.Build());
            Assert.AreEqual("not defaultA",
                            i.GetInstance<TwoNamedStringArgs>().a);
            Assert.AreEqual("not defaultB",
                            i.GetInstance<TwoNamedStringArgs>().b);
        }

        [TestMethod]
        public void TestTwoNamedStringArgsBindVolatile()
        {
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder();
            TwoNamedStringArgs a = tang.NewInjector(cb.Build()).GetInstance<TwoNamedStringArgs>();
            Assert.AreEqual("defaultA", a.a);
            Assert.AreEqual("defaultB", a.b);
            IInjector i = tang.NewInjector(cb.Build());
            i.BindVolatileParameter(GenericType<TwoNamedStringArgs.A>.Class, "not defaultA");
            i.BindVolatileParameter(GenericType<TwoNamedStringArgs.B>.Class, "not defaultB");
            Assert.AreEqual("not defaultA",
                            i.GetInstance<TwoNamedStringArgs>().a);
            Assert.AreEqual("not defaultB",
                            i.GetInstance<TwoNamedStringArgs>().b);
        }

        [TestMethod]
        public void TestTwoNamedStringArgsReBindVolatileFail()
        {
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder();
            TwoNamedStringArgs a = tang.NewInjector(cb.Build()).GetInstance<TwoNamedStringArgs>();
            Assert.AreEqual("defaultA", a.a);
            Assert.AreEqual("defaultB", a.b);
            cb.BindNamedParameter<TwoNamedStringArgs.A, string>(GenericType<TwoNamedStringArgs.A>.Class, "not defaultA");
            cb.BindNamedParameter<TwoNamedStringArgs.B, string>(GenericType<TwoNamedStringArgs.B>.Class, "not defaultB");
            IInjector i = tang.NewInjector(cb.Build());
            string msg = null;
            try
            {
                i.BindVolatileParameter(GenericType<TwoNamedStringArgs.A>.Class, "not defaultA");
                i.BindVolatileParameter(GenericType<TwoNamedStringArgs.B>.Class, "not defaultB");
                msg =
                    "Attempt to re-bind named parameter TwoNamedStringArgs+A.  Old value was [not defaultA] new value is [not defaultA]";
            }
            catch (Exception)
            {
            }
            Assert.IsNull(msg);
        }

        [TestMethod]
        public void TestBextendsAinjectA()
        {
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder();
            cb.BindImplementation(GenericType<BextendsAinjectA.A>.Class, GenericType<BextendsAinjectA.A>.Class);
            BextendsAinjectA.A a = tang.NewInjector(cb.Build()).GetInstance<BextendsAinjectA.A>();
            Assert.IsNotNull(a);
        }

        [TestMethod]
        public void TestNamedImpl()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder(new string[] { FileNames.Examples });
            cb.BindNamedParameter<AImplName, Aimpl, INamedImplA>(GenericType<AImplName>.Class, GenericType<Aimpl>.Class);
            cb.BindNamedParameter<BImplName, Bimpl, INamedImplA>(GenericType<BImplName>.Class, GenericType<Bimpl>.Class);

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            Aimpl a1 = (Aimpl) i.GetNamedInstance<AImplName, INamedImplA>(GenericType<AImplName>.Class);
            Aimpl a2 = (Aimpl) i.GetNamedInstance<AImplName, INamedImplA>(GenericType<AImplName>.Class);
            Bimpl b1 = (Bimpl) i.GetNamedInstance<BImplName, INamedImplA>(GenericType<BImplName>.Class);
            Bimpl b2 = (Bimpl) i.GetNamedInstance<BImplName, INamedImplA>(GenericType<BImplName>.Class);
            Assert.AreSame(a1, a2);
            Assert.AreSame(b1, b2);
        }

        [TestMethod]
        public void testThreeConstructors() 
        {
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder();
            cb.BindNamedParameter<ThreeConstructors.TCInt, Int32>(GenericType<ThreeConstructors.TCInt>.Class, "1");
            cb.BindNamedParameter<ThreeConstructors.TCString, string>(GenericType<ThreeConstructors.TCString>.Class, "s");
            ThreeConstructors tc = tang.NewInjector(cb.Build()).GetInstance<ThreeConstructors>();
            Assert.AreEqual(1, tc.i);
            Assert.AreEqual("s", tc.s);
    
            cb = tang.NewConfigurationBuilder();
            cb.BindNamedParameter<ThreeConstructors.TCInt, Int32>(GenericType<ThreeConstructors.TCInt>.Class, "1");
            tc = tang.NewInjector(cb.Build()).GetInstance<ThreeConstructors>();
            Assert.AreEqual(1, tc.i);
            Assert.AreEqual("default", tc.s);

            cb = tang.NewConfigurationBuilder();
            cb.BindNamedParameter<ThreeConstructors.TCString, string>(GenericType<ThreeConstructors.TCString>.Class, "s");
            tc = tang.NewInjector(cb.Build()).GetInstance<ThreeConstructors>();
            Assert.AreEqual(-1, tc.i);
            Assert.AreEqual("s", tc.s);

            cb = tang.NewConfigurationBuilder();
            cb.BindNamedParameter<ThreeConstructors.TCFloat, float>(GenericType<ThreeConstructors.TCFloat>.Class, "2");
            tc = tang.NewInjector(cb.Build()).GetInstance<ThreeConstructors>();
            Assert.AreEqual(-1, tc.i);
            Assert.AreEqual("default", tc.s);
            Assert.AreEqual(2.0f, tc.f, 1e-9);
        }

        [TestMethod]
        public void TestThreeConstructorsAmbiguous()
        {
            string msg = null;

            try
            {
                ICsConfigurationBuilder cb = tang.NewConfigurationBuilder();
                cb.BindNamedParameter<ThreeConstructors.TCString, string>(GenericType<ThreeConstructors.TCString>.Class, "s");
                cb.BindNamedParameter<ThreeConstructors.TCFloat, float>(GenericType<ThreeConstructors.TCFloat>.Class, "-2");

                // Ambiguous; there is a constructor that takes a string, and another that
                // takes a float, but none that takes both.
                tang.NewInjector(cb.Build()).GetInstance<ThreeConstructors>();
                msg = @"Cannot inject Org.Apache.REEF.Tang.Tests.Tang.ThreeConstructors, Org.Apache.REEF.Tang.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null " + 
                    "Ambiguous subplan Org.Apache.REEF.Tang.Tests.Tang.ThreeConstructors, Org.Apache.REEF.Tang.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null " + 
                    "new Org.Apache.REEF.Tang.Tests.Tang.ThreeConstructors(System.String Org.Apache.REEF.Tang.Tests.Tang.ThreeConstructors+TCString = s) " + 
                    "new Org.Apache.REEF.Tang.Tests.Tang.ThreeConstructors(System.Single Org.Apache.REEF.Tang.Tests.Tang.ThreeConstructors+TCFloat = -2) ";
            }
            catch (InjectionException e)
            {
                System.Diagnostics.Debug.WriteLine(e);
            }
            Assert.IsNull(msg);
        }

        [TestMethod]
        public void TestTwoConstructorsAmbiguous()
        {
            string msg = null;
            try
            {
                ICsConfigurationBuilder cb = tang.NewConfigurationBuilder();
                cb.BindNamedParameter<TwoConstructors.TCInt, Int32>(GenericType<TwoConstructors.TCInt>.Class, "1");
                cb.BindNamedParameter<ThreeConstructors.TCString, string>(GenericType<ThreeConstructors.TCString>.Class, "s");
                tang.NewInjector(cb.Build()).GetInstance<TwoConstructors>();
                msg = @"Cannot inject Org.Apache.REEF.Tang.Tests.Tang.TwoConstructors, Org.Apache.REEF.Tang.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null "+
                "Ambiguous subplan Org.Apache.REEF.Tang.Tests.Tang.TwoConstructors, Org.Apache.REEF.Tang.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null" +
                "new Org.Apache.REEF.Tang.Tests.Tang.TwoConstructors(System.Int32 Org.Apache.REEF.Tang.Tests.Tang.TwoConstructors+TCInt = 1, System.String Org.Apache.REEF.Tang.Tests.Tang.TwoConstructors+TCString = s)" +
                "new Org.Apache.REEF.Tang.Tests.Tang.TwoConstructors(System.String Org.Apache.REEF.Tang.Tests.Tang.TwoConstructors+TCString = s, System.Int32 Org.Apache.REEF.Tang.Tests.Tang.TwoConstructors+TCInt = 1)";
            }
            catch (InjectionException e)
            {
                System.Diagnostics.Debug.WriteLine(e); 
            }
            Assert.IsNull(msg);
        }

        [TestMethod]
        public void TestSingletonWithMultipleConstructors() 
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindImplementation(GenericType<SMC>.Class, GenericType<SingletonMultiConst>.Class);
            cb.BindNamedParameter<SingletonMultiConst.A, string>(GenericType<SingletonMultiConst.A>.Class, "foo");
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<SMC>();
            Assert.IsNotNull(o);
        }

        [TestMethod]
        public void TestSingletonWithMoreSpecificConstructors()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindImplementation(GenericType<SMC>.Class, GenericType<SingletonMultiConst>.Class);
            cb.BindNamedParameter<SingletonMultiConst.A, string>(GenericType<SingletonMultiConst.A>.Class, "foo");
            cb.BindNamedParameter<SingletonMultiConst.B, string>(GenericType<SingletonMultiConst.B>.Class, "bar");
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<SMC>();
            Assert.IsNotNull(o);
        }

        [TestMethod]
        public void TestInjectInjector()
        {
            IInjector i = TangFactory.GetTang().NewInjector();
            var ii = (InjectInjector) i.GetInstance(typeof(InjectInjector));
            //Assert.IsTrue(ii.i is IInjector);
            Assert.AreNotSame(i, ii.i);
        }

        [TestMethod]
        public void TestGenericEventHandlers()
        {
            ICsConfigurationBuilder cba = TangFactory.GetTang().NewConfigurationBuilder();
            cba.BindNamedParameter<ABCName.XName, ABCName.XXBB, ABCName.X<ABCName.BB>>(GenericType<ABCName.XName>.Class, GenericType<ABCName.XXBB>.Class);
            TangFactory.GetTang().NewInjector(cba.Build()).GetNamedInstance(typeof(ABCName.XName));

            ICsConfigurationBuilder cbb = TangFactory.GetTang().NewConfigurationBuilder();
            cbb.BindNamedParameter<ABCName.XName, ABCName.XBB, ABCName.X<ABCName.BB>>(GenericType<ABCName.XName>.Class, GenericType<ABCName.XBB>.Class);
            TangFactory.GetTang().NewInjector(cbb.Build()).GetNamedInstance(typeof(ABCName.XName));
        }

        [TestMethod]
        public void TestGenericEventHandlerDefaults() 
        {
            ICsConfigurationBuilder cba = TangFactory.GetTang().NewConfigurationBuilder();
            var xbb = TangFactory.GetTang().NewInjector(cba.Build()).GetNamedInstance(typeof(ABCName.XNameDB));
            Assert.IsTrue(xbb is ABCName.XBB);
        }

        [TestMethod]
        public void TestGenericEventHandlerDefaultsGoodTreeIndirection() 
        {
            ICsConfigurationBuilder cba = TangFactory.GetTang().NewConfigurationBuilder();
            var o = TangFactory.GetTang().NewInjector(cba.Build()).GetNamedInstance(typeof(ABCName.XNameDDAA));
            Assert.IsTrue(o is ABCName.XXBB);
        }

        [TestMethod]
        public void TestGenericUnrelatedGenericTypeParameters() 
        {
            string msg = null;
            try
            {
                ICsConfigurationBuilder cba = TangFactory.GetTang().NewConfigurationBuilder();
                TangFactory.GetTang().NewInjector(cba.Build()).GetNamedInstance(typeof(WaterBottleName));
                msg =
                    "class WaterBottleName defines a default class GasCan with a type that does not extend its target's type Water";
            }
            catch (ClassHierarchyException e)
            {
                System.Diagnostics.Debug.WriteLine(e);
            }    
            Assert.IsNull(msg);        
        }

        [TestMethod]
        public void TestGenericInterfaceUnboundTypeParametersName()
        {
            ICsConfigurationBuilder cba = TangFactory.GetTang().NewConfigurationBuilder();
            var o = TangFactory.GetTang().NewInjector(cba.Build()).GetNamedInstance(typeof(FooEventHandler));
            Assert.IsTrue(o is MyEventHandler<Foo>);
        }

        [TestMethod]
        public void TestGenericInterfaceUnboundTypeParametersNameIface()
        {
            ICsConfigurationBuilder cba = TangFactory.GetTang().NewConfigurationBuilder();
            var o = TangFactory.GetTang().NewInjector(cba.Build()).GetNamedInstance(typeof(IfaceEventHandler));
            Assert.IsTrue(o is IEventHandler<SomeIface>);
        }

        [TestMethod]
        public void TestGenericInterfaceUnboundTypeParametersIface()
        {
            string msg = null;
            try
            {
                ICsConfigurationBuilder cba = TangFactory.GetTang().NewConfigurationBuilder();
                TangFactory.GetTang().NewInjector(cba.Build()).IsInjectable(typeof(MyEventHandlerIface));
                msg =
                    "interface MyEventHandlerIface declares its default implementation to be non-subclass class MyEventHandler";
            }
            catch (ClassHierarchyException e)
            {
                System.Diagnostics.Debug.WriteLine(e);
            }    
            Assert.IsNull(msg);    
        }

        [TestMethod]
        public void TestWantSomeHandlers() 
        {
            var o = TangFactory.GetTang().NewInjector().GetInstance<WantSomeHandlers>();
            Assert.IsNotNull(o);
        }
        
        [TestMethod]
        public void TestWantSomeHandlersBadOrder() 
        {
            IInjector i = TangFactory.GetTang().NewInjector();
            var o1 = i.GetInstance<IAHandler>();
            var o2 = i.GetInstance<IBHandler>();
            var o3 = i.GetInstance<WantSomeFutureHandlers>();
            Assert.IsTrue(o1 is AHandlerImpl);            
            Assert.IsTrue(o2 is BHandlerImpl);
            Assert.IsNotNull(o3);
        }

        [TestMethod]
        public void TestWantSomeFutureHandlersAlreadyBoundVolatile() 
        {
            IInjector i = TangFactory.GetTang().NewInjector();
            i.BindVolatileInstance(GenericType<IAHandler>.Class, new AHandlerImpl());
            i.BindVolatileInstance(GenericType<IBHandler>.Class, new BHandlerImpl());
            i.GetInstance<WantSomeFutureHandlers>();
        }

        [TestMethod]
        public void TestWantSomeFutureHandlers() 
        {
            TangFactory.GetTang().NewInjector().GetInstance<WantSomeFutureHandlers>();
        }

        [TestMethod]
        public void TestWantSomeFutureHandlersName() 
        {
            TangFactory.GetTang().NewInjector().GetInstance<WantSomeFutureHandlersName>();
        }

        [TestMethod]
        public void TestReuseFailedInjector() 
        {
            IInjector i = TangFactory.GetTang().NewInjector();
            try 
            {
                i.GetInstance<Fail>();
                Assert.Fail("Injecting Fail should not have worked!");
            } catch (InjectionException) 
            {
                 i.GetInstance<Pass>();
            }
        }

        [TestMethod]
        public void TestMultipleLayersFromAbstractClass()
        {
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder();
            cb.BindImplementation(GenericType<MultiLayer>.Class, GenericType<LowerLayer>.Class);
            MultiLayer o = tang.NewInjector(cb.Build()).GetInstance<MultiLayer>();
            Assert.IsNotNull(o);
        }

        [TestMethod]
        public void TestMultipleLayersFromInterface()
        {
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder();
            cb.BindImplementation(GenericType<IMultiLayer>.Class, GenericType<LowerLayerImpl>.Class);
            IMultiLayer o = tang.NewInjector(cb.Build()).GetInstance<IMultiLayer>();
            Assert.IsNotNull(o);
        }

        [TestMethod]
        public void TestEmptyStringAsDefaultValue()
        {
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(EmptyStringAsDefaultParamConf.ConfigurationModule.Build());
            var value = tang.NewInjector(cb.Build()).GetNamedInstance<EmptyStringAsDefaultParam, string>();
            Assert.IsNotNull(value.Equals(""));
        }
    }

    internal class InjectInjector
    {
        public IInjector i;

        [Inject]
        private InjectInjector(IInjector i)
        {
            this.i = i;
        }
    }

    internal class MustBeSingleton
    {
        public static bool alreadyInstantiated;

        [Inject]
        public MustBeSingleton()
        {
            if (alreadyInstantiated)
            {
                throw new IllegalStateException("Can't instantiate me twice!");
            }
            alreadyInstantiated = true;
        }
    }

    internal class SubSingleton
    {
        [Inject]
        private SubSingleton(MustBeSingleton a)
        {
            // Does not call super
        }
    }

    internal class TwoSingletons
    {
        [Inject]
        private TwoSingletons(SubSingleton a, MustBeSingleton b)
        {
        }
    }

    internal class RepeatedAmbiguousArgs
    {
        [Inject]
        private RepeatedAmbiguousArgs(int x, int y)
        {
        }
    }

    public class RepeatedNamedArgs
    {
        [NamedParameter]
        public class A : Name<Int32>
        {
        }

        [NamedParameter()]
        public class B : Name<Int32>
        {
        }

        [Inject]
        public RepeatedNamedArgs([Parameter(typeof (A))] int x, [Parameter(Value = typeof (B))] int y)
        {
        }
    }

    internal class RepeatedNamedSingletonArgs
    {
        [NamedParameter()]
        public class A : Name<MustBeSingleton>
        {
        }

        [NamedParameter()]
        public class B : Name<MustBeSingleton>
        {
        }

        [Inject]
        public RepeatedNamedSingletonArgs([Parameter(typeof (A))] MustBeSingleton a,
                                          [Parameter(typeof (B))] MustBeSingleton b)
        {
        }
    }

    internal class OneNamedSingletonArgs
    {
        [NamedParameter()]
        public class A : Name<MustBeSingleton>
        {
        }

        [NamedParameter()]
        public class B : Name<MustBeSingleton>
        {
        }

        [Inject]
        public OneNamedSingletonArgs([Parameter(typeof (A))] MustBeSingleton a)
        {
        }
    }

    [NamedParameter(Documentation = "woo", ShortName = "woo", DefaultValue = "42")]
    internal class Param : Name<Int32>
    {
    }

    internal interface Interf
    {
    }

    internal class Impl : Interf
    {
        [Inject]
        private Impl([Parameter(Value = typeof (Param))] int p)
        {
        }
    }

    internal class OneNamedStringArg
    {
        [NamedParameter(DefaultValue = "default")]
        public class A : Name<string>
        {
        }

        public string s;

        [Inject]
        private OneNamedStringArg([Parameter(typeof (A))] string s)
        {
            this.s = s;
        }
    }

    internal class TwoNamedStringArgs
    {
        [NamedParameter(DefaultValue = "defaultA")]
        public class A : Name<string>
        {
        }

        [NamedParameter(DefaultValue = "defaultB")]
        public class B : Name<string>
        {
        }

        public string a;
        public string b;

        [Inject]
        private TwoNamedStringArgs([Parameter(typeof (A))] string a, [Parameter(typeof (B))] String b)
        {
            this.a = a;
            this.b = b;
        }
    }

    internal class BextendsAinjectA
    {
        public class A
        {
            [Inject]
            public A()
            {
            }
        }

        public class B : A
        {
        }
    }

    public interface INamedImplA
    {
    }

    public interface INamedImplC
    {
    }

    [NamedParameter]
    public class AImplName : Name<INamedImplA>
    {
    }

    [NamedParameter]
    public class BImplName : Name<INamedImplA>
    {
    }

    [NamedParameter]
    public class CImplName : Name<INamedImplC>
    {
    }

    public class Aimpl : INamedImplA
    {
        [Inject]
        private Aimpl()
        {
        }
    }

    public class Bimpl : INamedImplA
    {
        [Inject]
        private Bimpl()
        {
        }
    }

    public class Cimpl : INamedImplC
    {
        [Inject]
        private Cimpl()
        {
        }
    }

    internal class NamedImpl
    {
        [NamedParameter]
        public class AImplName : Name<A>
        {
        }

        [NamedParameter]
        public class BImplName : Name<A>
        {
        }

        [NamedParameter]
        public class CImplName : Name<C>
        {
        }

        public interface A
        {
        }

        public interface C
        {
        }

        public class Aimpl : A
        {
            [Inject]
            private Aimpl()
            {
            }
        }

        public class Bimpl : A
        {
            [Inject]
            private Bimpl()
            {
            }
        }

        public class Cimpl : C
        {
            [Inject]
            private Cimpl()
            {
            }
        }

        public class ABtaker
        {
            [Inject]
            private ABtaker([Parameter(typeof (AImplName))] INamedImplA a, [Parameter(typeof (BImplName))] INamedImplA b)
            {
                //Assert.IsTrue(a is Aimpl, "AImplName must be instance of Aimpl");
                //Assert.IsTrue(b is Bimpl, "BImplName must be instance of Bimpl");
            }
        }
    }

    [NamedParameter(DefaultValue = "")]
    public class EmptyStringAsDefaultParam : Name<string>
    {
    }

    public class EmptyStringAsDefaultParamConf : ConfigurationModuleBuilder
    {
        public static readonly OptionalParameter<string> OptionalString = new OptionalParameter<string>();

        public static ConfigurationModule ConfigurationModule = new EmptyStringAsDefaultParamConf()
            .BindNamedParameter(GenericType<EmptyStringAsDefaultParam>.Class, OptionalString)
            .Build();
    }

    [NamedParameter]
    public class StringWithoutDefaultParam : Name<string>
    {
    }

    public class StringWithoutDefaultParamConf : ConfigurationModuleBuilder
    {
        public static readonly OptionalParameter<string> OptionalString = new OptionalParameter<string>();

        public static ConfigurationModule ConfigurationModule = new StringWithoutDefaultParamConf()
            .BindNamedParameter(GenericType<StringWithoutDefaultParam>.Class, OptionalString)
            .Build();
    }

    class ThreeConstructors
    {
        public int i;
        public string s;
        public float f;

        [NamedParameter]
        public class TCInt : Name<Int32> {}

        [NamedParameter]
        public class TCString : Name<string> { }

        [NamedParameter]
        public class TCFloat : Name<float> {}

        [Inject]
        public ThreeConstructors([Parameter(typeof(TCInt))] int i, [Parameter(typeof(TCString))] string s) 
        { 
            this.i = i;
            this.s = s;
            this.f = -1.0f;
        }

        [Inject]
        public ThreeConstructors([Parameter(typeof(TCString))] string s) : this(-1, s)
        {
        }

        [Inject]
        public ThreeConstructors([Parameter(typeof(TCInt))] int i) : this(i, "default")
        {
        }

        [Inject]
        public ThreeConstructors([Parameter(typeof(TCFloat))] float f) 
        {
            this.i = -1;
            this.s = "default";
            this.f = f;
        } 
    }

    class TwoConstructors
    {
        public int i;
        public string s;

        [NamedParameter]
        public class TCInt : Name<Int32> { }

        [NamedParameter]
        public class TCString : Name<string> { }


        [Inject]
        public TwoConstructors([Parameter(typeof(TCInt))] int i, [Parameter(typeof(TCString))] string s)
        {
            this.i = i;
            this.s = s;
        }

        [Inject]
        public TwoConstructors([Parameter(typeof(TCString))] string s, [Parameter(typeof(TCInt))] int i)
        {
            this.i = i;
            this.s = s;
        }
    }

    interface SMC { }

    class SingletonMultiConst : SMC 
    {
        [NamedParameter]
        public class A : Name<string> { }
  
        [NamedParameter]
        public class B : Name<string> { }
  
        [Inject]
        public SingletonMultiConst([Parameter(typeof(A))] String a) { }
        
        [Inject]
        public SingletonMultiConst([Parameter(typeof(A))] string a, [Parameter(typeof(B))] string b) { }
    }

    internal class ABCName
    {
        public interface X<T>
        {
        }

        [NamedParameter]
        public class XName : Name<X<BB>>
        {
        }

        //[NamedParameter(DefaultClass = typeof(XAA))]
        //public class XNameDA : Name<X<BB>>
        //{
        //}

        [NamedParameter(DefaultClass = typeof(XBB))]
        public class XNameDB : Name<X<BB>>
        {
        }

        //[NamedParameter(DefaultClass = typeof(XCC))]
        //public class XNameDC : Name<X<BB>>
        //{
        //}

        //[NamedParameter(DefaultClass = typeof(XCC))]
        //public class XNameDAA : Name<XBB>
        //{
        //}

        [NamedParameter(DefaultClass = typeof(XXBB))]
        public class XNameDDAA : Name<XBB>
        {
        }

        [DefaultImplementation(typeof(AA))]
        public class AA
        {
            [Inject]
            public AA()
            {
            }
        }

        [DefaultImplementation(typeof(BB))]
        public class BB : AA
        {
            [Inject]
            public BB()
            {
            }
        }

        [DefaultImplementation(typeof(CC))]
        public class CC : BB
        {
            [Inject]
            public CC()
            {
            }
        }

        public class XAA : X<AA>
        {
            [Inject]
            public XAA(AA aa)
            {
            }
        }

        [DefaultImplementation(typeof(XBB))]
        public class XBB : X<BB>
        {
            [Inject]
            public XBB(BB aa)
            {
            }
        }

        public class XXBB : XBB
        {
            [Inject]
            public XXBB(BB aa)
                : base(aa)
            {
            }
        }

        public class XCC : X<CC>
        {
            [Inject]
            public XCC(CC aa)
            {
            }
        }
    }

    interface Bottle<Y> {
  
    }
    class WaterBottle : Bottle<Water> 
    {  
    }
    class GasCan : Bottle<Gas> 
    {  
    }
    class Water {}
    class Gas {}

    [NamedParameter(DefaultClass=typeof(GasCan))]
    class WaterBottleName : Name<Bottle<Water>> { }

    interface IEventHandler <T> { }
    class MyEventHandler<T> : IEventHandler<T> 
    { 
        [Inject]
        MyEventHandler () { }
    }

    [DefaultImplementation(typeof(MyEventHandler<Foo>))]
    interface MyEventHandlerIface : IEventHandler<Foo> { }

    [NamedParameter(DefaultClass = typeof(MyEventHandler<Foo>))]
    class FooEventHandler : Name<IEventHandler<Foo>> { }

    internal class Foo : Name<String>
    {
    }

    interface SomeIface { }
    [NamedParameter(DefaultClass = typeof(MyEventHandler<SomeIface>))]
    class IfaceEventHandler : Name<IEventHandler<SomeIface>> { }

    class AH
    {
        [Inject]
        AH() {}
    }
    class BH
    {
        [Inject]
        BH() {}
    }

    [DefaultImplementation(typeof(AHandlerImpl))]
    interface IAHandler : IEventHandler<AH> { }

    [DefaultImplementation(typeof(BHandlerImpl))]
    interface IBHandler : IEventHandler<BH> { }

    class AHandlerImpl : IAHandler
    {
        [Inject]
        public AHandlerImpl() { }
    }
    class BHandlerImpl : IBHandler 
    {
        [Inject]
        public BHandlerImpl() { }
    }

    class WantSomeHandlers 
    {
        [Inject]
        WantSomeHandlers(IAHandler a, IBHandler b) { }
    }
    class WantSomeFutureHandlers 
    {
        [Inject]
        WantSomeFutureHandlers(IInjectionFuture<IAHandler> a, IInjectionFuture<IBHandler> b) { }
    }

    [NamedParameter(DefaultClass = typeof(AHandlerImpl))]
    class AHandlerName : Name<IEventHandler<AH>> { }
    
    [NamedParameter(DefaultClass = typeof(BHandlerImpl))]
    class BHandlerName : Name<IEventHandler<BH>> { }

    class WantSomeFutureHandlersName 
    {
        [Inject]
        WantSomeFutureHandlersName(
            [Parameter(typeof (AHandlerName))] IInjectionFuture<IEventHandler<AH>> a,
            [Parameter(typeof (BHandlerName))] IInjectionFuture<IEventHandler<BH>> b)
        {            
        }
    }

    class Pass 
    {
        [Inject]
        public Pass()
        {            
        }
    }

    class Fail 
    {
        [Inject]
        public Fail()
        {
            throw new NotSupportedException();
        }
    }

    abstract class MultiLayer
    {
         
    }

    class MiddleLayer : MultiLayer
    {
       [Inject]
        public MiddleLayer() { }
    }

    class LowerLayer : MiddleLayer 
    {
       [Inject]
        public LowerLayer() { }
    }

    interface IMultiLayer
    {

    }

    class MiddleLayerImpl : IMultiLayer
    {
        [Inject]
        public MiddleLayerImpl() { }
    }

    class LowerLayerImpl : MiddleLayerImpl
    {
        [Inject]
        public LowerLayerImpl() { }
    }
}