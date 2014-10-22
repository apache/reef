/**
 * Copyright (C) 2014 Microsoft Corporation
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
﻿using System;
using System.Collections;
using System.Collections.Generic;
using Com.Microsoft.Tang.Exceptions;
using Com.Microsoft.Tang.formats;
using Com.Microsoft.Tang.Interface;
using Com.Microsoft.Tang.Protobuf;
using Com.Microsoft.Tang.Util;

namespace Com.Microsoft.Tang.Implementations
{
    public class TangImpl : ITang
    {
        private static IDictionary<SetValuedKey, ICsClassHierarchy> defaultClassHierarchy = new Dictionary<SetValuedKey, ICsClassHierarchy>();

        public IInjector NewInjector()
        {
            try
            {
                return NewInjector(new ConfigurationImpl[] {});
            }
            catch (BindException e)
            {
                throw new IllegalStateException("Unexpected error from empty configuration", e);
            }
        }

        public IInjector NewInjector(string[] assemblies, string configurationFileName)
        {
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb1 = tang.NewConfigurationBuilder(assemblies);
            ConfigurationFile.AddConfiguration(cb1, configurationFileName);
            IConfiguration conf = cb1.Build();

            IInjector injector = tang.NewInjector(conf);
            return injector;
        }

        public IInjector NewInjector(string[] assemblies, IDictionary<string, string> configurations)
        {
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb1 = tang.NewConfigurationBuilder(assemblies);
            ConfigurationFile.AddConfiguration(cb1, configurations);
            IConfiguration conf = cb1.Build();

            IInjector injector = tang.NewInjector(conf);
            return injector;
        }

        public IInjector NewInjector(IConfiguration[] confs)
        {
            return new InjectorImpl(new CsConfigurationBuilderImpl(confs).Build());
        }

        public IInjector NewInjector(IConfiguration conf)
        {
            return new InjectorImpl(conf);
            //try
            //{
            //    return NewInjector(new ConfigurationImpl[] { (ConfigurationImpl)conf });
            //}
            //catch (BindException e)
            //{
            //    throw new IllegalStateException("Unexpected error cloning configuration", e);
            //}
        }

        public IClassHierarchy GetClassHierarchy(string[] assemblies)
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
                throw new IllegalStateException(
                    "Caught unexpeceted bind exception!  Implementation bug.", e);
            }
        }

        public ICsConfigurationBuilder NewConfigurationBuilder(string[] assemblies)
        {
            try
            {
                return NewConfigurationBuilder(assemblies, new IConfiguration[0], new Type[0]);
            }
            catch (BindException e)
            {
                throw new IllegalStateException(
                    "Caught unexpeceted bind exception!  Implementation bug.", e);
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

        public ICsConfigurationBuilder NewConfigurationBuilder(IConfiguration[] confs)
        {
            return NewConfigurationBuilder(new string[0], confs, new Type[0]);
        }

        public ICsConfigurationBuilder NewConfigurationBuilder(string[] assemblies, IConfiguration[] confs, Type[] parameterParsers)
        {
            return new CsConfigurationBuilderImpl(assemblies, confs, parameterParsers);
        }

        public static void Reset() 
        {
            defaultClassHierarchy = new Dictionary<SetValuedKey, ICsClassHierarchy>(); 
        }
    }
}
