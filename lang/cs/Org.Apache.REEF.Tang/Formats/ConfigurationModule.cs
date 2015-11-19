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
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Formats
{
    /**
     * Allows applications to bundle sets of configuration options together into 
     * discrete packages.  Unlike more conventional approaches,
     * ConfigurationModules store such information in static data structures that
     * can be statically discovered and sanity-checked. 
     * 
     * @see Org.Apache.REEF.Tang.Formats.TestConfigurationModule for more information and examples.
     *
     */
    public class ConfigurationModule
    {
        public readonly ConfigurationModuleBuilder Builder;
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ConfigurationModule));
        private readonly MonotonicHashSet<FieldInfo> reqSet = new MonotonicHashSet<FieldInfo>();
        private readonly MonotonicHashMap<object, Type> setImpls = new MonotonicHashMap<object, Type>();
        private readonly MonotonicMultiHashMap<object, Type> setImplSets = new MonotonicMultiHashMap<object, Type>();
        private readonly MonotonicMultiHashMap<object, string> setLateImplSets = new MonotonicMultiHashMap<object, string>();
        private readonly MonotonicMultiHashMap<object, string> setParamSets = new MonotonicMultiHashMap<object, string>();
        private readonly MonotonicHashMap<object, string> setLateImpls = new MonotonicHashMap<object, string>();
        private readonly MonotonicHashMap<object, string> setParams = new MonotonicHashMap<object, string>();

        private readonly MonotonicHashMap<object, IList<Type>> setImplLists = new MonotonicHashMap<object, IList<Type>>();
        private readonly MonotonicHashMap<object, IList<string>> setParamLists = new MonotonicHashMap<object, IList<string>>();
        private readonly MonotonicHashMap<object, IList<string>> setLateImplLists = new MonotonicHashMap<object, IList<string>>();
        
        public ConfigurationModule(ConfigurationModuleBuilder builder) 
        {
            this.Builder = builder.DeepCopy();
        }

        public ConfigurationModule Set<T, U>(IImpl<T> opt, GenericType<U> impl) 
            where U : T 
        {
            Type implType = typeof(U);
            
            ConfigurationModule c = DeepCopy();
            c.ProcessSet(opt);
            if (c.Builder.SetOpts.Contains(opt)) 
            {
                c.setImplSets.Add(opt, implType);
            } 
            else 
            {
                c.setImpls.Add(opt, implType);
            }
            return c;
        }

        public ConfigurationModule Set<T>(IImpl<T> opt, string impl) 
        {
            ConfigurationModule c = DeepCopy();
            c.ProcessSet(opt);
            if (c.Builder.SetOpts.Contains(opt)) 
            {
                c.setLateImplSets.Add(opt, impl);
            } 
            else 
            {
                c.setLateImpls.Add(opt, impl);
            }
            return c;
        }

        public ConfigurationModule Set<T, U>(IParam<T> opt, GenericType<U> val) 
            where U : T
        {
            Type t = typeof(U);
            string n = ReflectionUtilities.GetAssemblyQualifiedName(t);
            return Set(opt, n);
        }

        public ConfigurationModule Set(IParam<bool> opt, bool val)
        {
            return Set(opt, val);
        }

        ////TODO
        ////public readonly ConfigurationModule set(Param<? extends Number> opt, Number val) 
        ////{
        ////   return set(opt, val);
        ////}

        public ConfigurationModule Set<T>(IParam<T> opt, string val) 
        {
            ConfigurationModule c = DeepCopy();
            c.ProcessSet(opt);
            if (c.Builder.SetOpts.Contains(opt)) 
            {
                c.setParamSets.Add(opt, val);
            } 
            else 
            {
                c.setParams.Add(opt, val);
            }
            return c;
        }

        public ConfigurationModule Set<T>(IImpl<IList<T>> opt, IList<string> impl)
        {
            ConfigurationModule c = DeepCopy();
            c.ProcessSet(opt);
            c.setLateImplLists.Add(opt, impl);
            return c;
        }

        public ConfigurationModule Set<T>(IParam<IList<T>> opt, IList<string> impl)
        {
            ConfigurationModule c = DeepCopy();
            c.ProcessSet(opt);
            c.setParamLists.Add(opt, impl);
            return c;
        }

        public ConfigurationModule Set<T>(IImpl<IList<T>> opt, IList<Type> impl)
        {
            ConfigurationModule c = DeepCopy();
            c.ProcessSet(opt);
            c.setImplLists.Add(opt, impl);
            return c;
        }

        public IConfiguration Build()
        {
            ConfigurationModule c = DeepCopy();
    
            if (!c.reqSet.ContainsAll(c.Builder.ReqDecl)) 
            {
                ISet<FieldInfo> missingSet = new MonotonicHashSet<FieldInfo>();
                foreach (FieldInfo f in c.Builder.ReqDecl) 
                {
                    if (!c.reqSet.Contains(f)) 
                    {
                        missingSet.Add(f);
                    }
                }
                var e = new BindException(
                    "Attempt to build configuration before setting required option(s): "
                    + Builder.ToString(missingSet));
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
            }
  
            foreach (Type clazz in c.Builder.FreeImpls.Keys) 
            {
                object i = c.Builder.FreeImpls.Get(clazz);
                if (c.setImpls.ContainsKey(i))
                {
                    var cb = (ICsInternalConfigurationBuilder)c.Builder.B;
                    cb.Bind(clazz, c.setImpls.Get(i));
                } 
                else if (c.setLateImpls.ContainsKey(i)) 
                {
                    c.Builder.B.Bind(ReflectionUtilities.GetAssemblyQualifiedName(clazz), c.setLateImpls.Get(i));
                } 
                else if (c.setImplSets.ContainsKey(i) || c.setLateImplSets.ContainsKey(i))
                {
                    foreach (Type clz in c.setImplSets.GetValuesForKey(i))
                    {
                        ICsInternalConfigurationBuilder b = (ICsInternalConfigurationBuilder)c.Builder.B;
                        b.BindSetEntry(clazz, clz);
                    }
                    foreach (string s in c.setLateImplSets.GetValuesForKey(i))
                    {
                        ICsInternalConfigurationBuilder b = (ICsInternalConfigurationBuilder)c.Builder.B;
                        b.BindSetEntry(clazz, s);
                    }
                } 
                else if (c.setImplLists.ContainsKey(i))
                {
                    ICsConfigurationBuilder b = (ICsConfigurationBuilder)c.Builder.B;
                    b.BindList(clazz, setImplLists.Get(i));
                }
                else if (c.setLateImplLists.ContainsKey(i))
                {
                    ICsInternalConfigurationBuilder b = (ICsInternalConfigurationBuilder)c.Builder.B;
                    b.BindList(clazz, setLateImplLists.Get(i));
                }
            }
            
            foreach (Type clazz in c.Builder.FreeParams.Keys) 
            {
                object p = c.Builder.FreeParams.Get(clazz);
                string s = c.setParams.Get(p);
                bool foundOne = false;
                if (s != null) 
                {
                    ICsConfigurationBuilder cb = c.Builder.B;
                    cb.BindNamedParameter(clazz, s);
                    foundOne = true;
                }

                IList<string> paramListStr = c.setParamLists.Get(p);
                if (paramListStr != null)
                {
                    ICsInternalConfigurationBuilder b = (ICsInternalConfigurationBuilder)c.Builder.B;
                    b.BindList(clazz, paramListStr);
                    foundOne = true;
                }

                foreach (string paramStr in c.setParamSets.GetValuesForKey(p)) 
                {
                    ICsInternalConfigurationBuilder b = (ICsInternalConfigurationBuilder)c.Builder.B;
                    b.BindSetEntry(clazz, paramStr);   
                    foundOne = true;
                }

                if (!foundOne) 
                {
                    if (!ReflectionUtilities.IsInstanceOfGeneric(p, typeof(OptionalParameter<>)))
                    {
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException(), LOGGER);
                    }
                }
            }
            return c.Builder.B.Build();
        }

        public void AssertStaticClean() 
        {
            if (!(
                setImpls.IsEmpty() &&
                setParams.IsEmpty()) &&
                setImplSets.IsEmpty() &&
                setLateImplSets.IsEmpty() &&
                setParamSets.IsEmpty() &&
                setImplLists.IsEmpty() &&
                setLateImplLists.IsEmpty() &&
                setParamLists.IsEmpty() &&
                setLateImpls.IsEmpty())
            {
                var e = new ClassHierarchyException("Detected statically set ConfigurationModule Parameter / Implementation.  set() should only be used dynamically.  Use bind...() instead.");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
            }
        }

        private ConfigurationModule DeepCopy()
        {
            ConfigurationModule cm = new ConfigurationModule(Builder.DeepCopy());
            cm.setImpls.AddAll(setImpls);
            cm.setParams.AddAll(setParams);
            cm.setImplSets.AddAll(setImplSets);
            cm.setParamSets.AddAll(setParamSets);
            cm.setLateImplSets.AddAll(setLateImplSets);
            cm.setImplLists.AddAll(setImplLists);
            cm.setParamLists.AddAll(setParamLists);
            cm.setLateImplLists.AddAll(setLateImplLists);
            cm.setLateImpls.AddAll(setLateImpls);
            cm.reqSet.AddAll(reqSet);
            return cm;
        }

        private void ProcessSet(object impl)
        {
            FieldInfo f;
            Builder.Map.TryGetValue(impl, out f);
            if (f == null)
            { 
                var e = new ClassHierarchyException("Unknown Impl/Param when setting " + impl.GetType().Name + ".  Did you pass in a field from some other module?");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
            }
            if (!reqSet.Contains(f))
            {
                reqSet.Add(f);
            }
        }
    }
}
