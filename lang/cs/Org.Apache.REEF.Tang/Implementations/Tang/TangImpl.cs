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
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.ClassHierarchy;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.InjectionPlan;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Implementations.Tang
{
    public class TangImpl : ITang
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(TangImpl));

        private static IDictionary<SetValuedKey, ICsClassHierarchy> defaultClassHierarchy = new Dictionary<SetValuedKey, ICsClassHierarchy>();

        public IInjector NewInjector()
        {
            try
            {
                return NewInjector(new ConfigurationImpl[] { });
            }
            catch (BindException e)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException("Unexpected error from empty configuration", e), LOGGER);
                return null;
            }
        }

        public IInjector NewInjector(string[] assemblies, string configurationFileName)
        {
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb1 = tang.NewConfigurationBuilder(assemblies);
            ConfigurationFile.AddConfigurationFromFile(cb1, configurationFileName);
            IConfiguration conf = cb1.Build();

            IInjector injector = tang.NewInjector(conf);
            return injector;
        }

        public ICsConfigurationBuilder NewConfigurationBuilder(IConfiguration conf)
        {
            return NewConfigurationBuilder(new IConfiguration[] { conf });
        }

        ////public IInjector NewInjector(string[] assemblies, IDictionary<string, string> configurations)
        ////{
        ////   ITang tang = TangFactory.GetTang();
        ////   ICsConfigurationBuilder cb1 = tang.NewConfigurationBuilder(assemblies);
        ////   ConfigurationFile.ProcessConfigData(cb1, configurations);
        ////   IConfiguration conf = cb1.Build();
        ////   IInjector injector = tang.NewInjector(conf);
        ////   return injector;
        ////}

        public IInjector NewInjector(string[] assemblies, IDictionary<string, string> configurations)
        {
            IList<KeyValuePair<string, string>> conf = new List<KeyValuePair<string, string>>();
            foreach (KeyValuePair<string, string> kp in configurations)
            {
                conf.Add(kp);
            }
            return NewInjector(assemblies, conf);
        }

        public IInjector NewInjector(string[] assemblies, IList<KeyValuePair<string, string>> configurations)
        {
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb1 = tang.NewConfigurationBuilder(assemblies);
            ConfigurationFile.ProcessConfigData(cb1, configurations);
            IConfiguration conf = cb1.Build();

            IInjector injector = tang.NewInjector(conf);
            return injector;
        }

        public IInjector NewInjector(params IConfiguration[] confs)
        {
            return new InjectorImpl(new CsConfigurationBuilderImpl(confs).Build());
        }

        public IInjector NewInjector(IConfiguration conf)
        {
            // return new InjectorImpl(conf);
            try
            {
                return NewInjector(new ConfigurationImpl[] { (ConfigurationImpl)conf });
            }
            catch (BindException e)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException("Unexpected error cloning configuration", e), LOGGER);
                return null; 
            }
        }

        public IClassHierarchy GetClassHierarchy(params string[] assemblies)
        {
            return GetDefaultClassHierarchy(assemblies, new Type[] { });
        }

        public ICsClassHierarchy GetDefaultClassHierarchy()
        {
            return GetDefaultClassHierarchy(new string[0], new Type[0]);
        }

        public ICsClassHierarchy GetDefaultClassHierarchy(string[] assemblies, Type[] parameterParsers)
        {
            SetValuedKey key = new SetValuedKey(assemblies, parameterParsers);

            ICsClassHierarchy ret = null;
            defaultClassHierarchy.TryGetValue(key, out ret);
            if (ret == null)
            {
                ret = new ClassHierarchyImpl(assemblies, parameterParsers);
                defaultClassHierarchy.Add(key, ret);
            }
            return ret;
        }

        public ICsConfigurationBuilder NewConfigurationBuilder()
        {
            try 
            {
                return NewConfigurationBuilder(new string[0], new IConfiguration[0], new Type[0]);
            } 
            catch (BindException e) 
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException(
                    "Caught unexpected bind exception!  Implementation bug.", e), LOGGER);
                return null;
            }
        }

        public ICsConfigurationBuilder NewConfigurationBuilder(params string[] assemblies)
        {
            try
            {
                return NewConfigurationBuilder(assemblies, new IConfiguration[0], new Type[0]);
            }
            catch (BindException e)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException(
                    "Caught unexpected bind exception!  Implementation bug.", e), LOGGER);
                return null;
            }
        }

        public IConfigurationBuilder NewConfigurationBuilder(IClassHierarchy classHierarchy)
        {
            return new ConfigurationBuilderImpl(classHierarchy);
        }

        public ICsConfigurationBuilder NewConfigurationBuilder(ICsClassHierarchy classHierarchy)
        {
            return new CsConfigurationBuilderImpl(classHierarchy);
        }

        public ICsConfigurationBuilder NewConfigurationBuilder(params IConfiguration[] confs)
        {
            return NewConfigurationBuilder(new string[0], confs, new Type[0]);
        }

        public ICsConfigurationBuilder NewConfigurationBuilder(string[] assemblies, IConfiguration[] confs, Type[] parameterParsers)
        {
            return new CsConfigurationBuilderImpl(assemblies, confs, parameterParsers);
        }

        public ICsConfigurationBuilder NewConfigurationBuilder(params Type[] parameterParsers) 
        {
            return NewConfigurationBuilder(new string[0], new IConfiguration[0], parameterParsers);
        }

        public static void Reset() 
        {
            defaultClassHierarchy = new Dictionary<SetValuedKey, ICsClassHierarchy>(); 
        }
    }
}
