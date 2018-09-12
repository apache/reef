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

using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Org.Apache.REEF.Tang.Formats
{
    public static class ConfigurationFile
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ConfigurationFile));

        #region text file serialization
        public static void WriteConfigurationFile(IConfiguration c, string fileName)
        {
            using (FileStream aFile = new FileStream(fileName, FileMode.Create))
            {
                using (StreamWriter sw = new StreamWriter(aFile))
                {
                    sw.Write(ToConfigurationString(c));
                }
            }
        }

        public static string ToConfigurationString(IConfiguration c) 
        {
            StringBuilder sb = new StringBuilder();
            foreach (string s in ToConfigurationStringList(c)) 
            {
                sb.Append(s);
                sb.Append('\n');
            }
            return sb.ToString();
        }

        private static string GetFullName(INode n)
        {
            string s = n.GetFullName();
            Type t = ReflectionUtilities.GetTypeByName(s);
            return t.FullName;
        }
    
        private static string GetFullName(string name)
        {
            try
            {
                Type t = ReflectionUtilities.GetTypeByName(name);
                return t.FullName;
            }
            catch (TangApplicationException e)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Warning, LOGGER);
                return name; // if name is not a type, return as it was
            }
        }

        private static string GetAssemblyName(string s)
        {
            try
            {
                Type t = ReflectionUtilities.GetTypeByName(s);
                return ReflectionUtilities.GetAssemblyQualifiedName(t);
            }
            catch (TangApplicationException e)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Warning, LOGGER);
                return s; // if name is not a type, return as it was
            }
        }

        public static HashSet<string> ToConfigurationStringList(IConfiguration c) 
        {
            ConfigurationImpl conf = (ConfigurationImpl)c;
            HashSet<string> l = new HashSet<string>();
            foreach (IClassNode opt in conf.GetBoundImplementations()) 
            {
                l.Add(GetFullName(opt) + '=' + Escape(GetFullName(conf.GetBoundImplementation(opt))));
            }
            
            foreach (IClassNode opt in conf.GetBoundConstructors()) 
            {
                l.Add(GetFullName(opt) + '=' + Escape(GetFullName(conf.GetBoundConstructor(opt))));
            }
            foreach (INamedParameterNode opt in conf.GetNamedParameters()) 
            {
                l.Add(GetFullName(opt) + '=' + Escape(GetFullName(conf.GetNamedParameter(opt))));
            }
            foreach (IClassNode cn in conf.GetLegacyConstructors())
            {
                StringBuilder sb = new StringBuilder();
                Join(sb, "-", conf.GetLegacyConstructor(cn).GetArgs().ToArray<IConstructorArg>());
                l.Add(GetFullName(cn) + Escape('=' + ConfigurationBuilderImpl.INIT + '(' + sb.ToString() + ')'));
            }

            IEnumerator bs = conf.GetBoundSets();
            while (bs.MoveNext())
            {
                KeyValuePair<INamedParameterNode, object> e = (KeyValuePair<INamedParameterNode, object>)bs.Current;

                string val = null;
                if (e.Value is string) 
                {
                    val = GetFullName((string)e.Value);
                } 
                else if (e.Value is INode) 
                {
                    val = GetFullName((INode)e.Value);
                } 
                else 
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException(), LOGGER);
                }
                
                l.Add(GetFullName(e.Key) + '=' + Escape(val));
            }

            IEnumerator bl = conf.GetBoundList().GetEnumerator();
            while (bl.MoveNext())
            {
                KeyValuePair<INamedParameterNode, IList<object>> e = (KeyValuePair<INamedParameterNode, IList<object>>)bl.Current;
                foreach (var item in e.Value)
                {
                    string val = null;
                    if (item is string)
                    {
                        val = GetFullName((string)item);
                    }
                    else if (e.Value is INode)
                    {
                        val = GetFullName((INode)e.Value);
                    }
                    else
                    {
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException(), LOGGER);
                    }
                    l.Add(GetFullName(e.Key) + '=' + Escape(val));
                }
            }

            return l;
        }

        public static IConfiguration GetConfiguration(string configString)
        {
            byte[] array = Encoding.GetEncoding(0).GetBytes(configString);
            return GetConfiguration(array);
        }

        public static IConfiguration GetConfiguration(byte[] configStream)
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            AddConfigurationFromStream(cb, configStream);
            return cb.Build();
        }

        public static void AddConfigurationFromStream(IConfigurationBuilder conf, byte[] configData)
        {
            using (StreamReader reader = new StreamReader(new MemoryStream(configData), Encoding.GetEncoding(0)))
            {
                 AddConfiguration(conf, reader);
            }
        }

        public static void AddConfigurationFromFile(IConfigurationBuilder conf, string configFileName)
        {  
            using (StreamReader reader = File.OpenText(configFileName))
            {
                AddConfiguration(conf, reader);
            }
        }

        public static void AddConfigurationFromString(IConfigurationBuilder conf, string configData)
        {
            byte[] array = Encoding.ASCII.GetBytes(configData);
            AddConfigurationFromStream(conf, array);
        }

        private static void AddConfiguration(IConfigurationBuilder conf, StreamReader reader)
        {
            IList<KeyValuePair<string, string>> settings = new List<KeyValuePair<string, string>>();

            while (!reader.EndOfStream)
            {
                string line = reader.ReadLine();
                string[] p = line.Split('=');
                if (p.Length == 2)
                {
                    settings.Add(new KeyValuePair<string, string>(GetAssemblyName(p[0]), GetAssemblyName(p[1])));
                } 
                else if (p.Length > 2)
                {
                    string v = line.Substring(p[0].Length + 1, line.Length - p[0].Length - 1);
                    settings.Add(new KeyValuePair<string, string>(GetAssemblyName(p[0]), GetAssemblyName(v)));
                }
                else
                {
                    var e = new TangApplicationException("Config data is not in format of KeyValuePair: " + line);
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }  
            }
            ProcessConfigData(conf, settings);
        }

        public static IDictionary<string, string> FromFile(string fileName)
        {
            IDictionary<string, string> property = new Dictionary<string, string>();
            using (StreamReader sr = File.OpenText(fileName))
            {
                while (!sr.EndOfStream)
                {
                    string line = sr.ReadLine();
                    string[] p = line.Split('=');
                    property.Add(ConfigurationFile.GetAssemblyName(p[0]), ConfigurationFile.GetAssemblyName(p[1]));
                }
            }
            return property;
        }

        public static void ProcessConfigData(IConfigurationBuilder conf, IList<KeyValuePair<string, string>> settings, string language)
        {
            foreach (KeyValuePair<string, string> kv in settings)
            {
                try
                {
                    conf.Bind(kv.Key, kv.Value, language);
                }
                catch (BindException ex)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(ex, Level.Error, LOGGER);
                    var e = new BindException("Failed to process configuration tuple: [" + kv.Key + "=" + kv.Value + "]", ex);
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
                catch (ClassHierarchyException ex)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(ex, Level.Error, LOGGER);
                    var e = new ClassHierarchyException("Failed to process configuration tuple: [" + kv.Key + "=" + kv.Value + "]", ex);
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
            }
        }

        internal static void ProcessConfigData(IConfigurationBuilder conf, IList<KeyValuePair<string, string>> settings)
        {
            foreach (KeyValuePair<string, string> kv in settings)
            {
                try
                {
                    conf.Bind(kv.Key, kv.Value);
                }
                catch (BindException ex)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(ex, Level.Error, LOGGER);
                    var e = new BindException("Failed to process configuration tuple: [" + kv.Key + "=" + kv.Value + "]", ex);
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
                catch (ClassHierarchyException ex)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(ex, Level.Error, LOGGER);
                    var e = new ClassHierarchyException("Failed to process configuration tuple: [" + kv.Key + "=" + kv.Value + "]", ex);
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
            }
        }

        public static void ProcessConfigData(IConfigurationBuilder conf, IDictionary<string, string> settings)
        {
            IList<KeyValuePair<string, string>> list = new List<KeyValuePair<string, string>>();

            foreach (KeyValuePair<string, string> kv in settings)
            {
                list.Add(kv);
            }

            ProcessConfigData(conf, list);
        }
 
       /**
        * Replace any \'s in the input string with \\. and any "'s with \".
        * @param in
        * @return
        */
        private static string Escape(string str) 
        {
            return str;  // TODO
            // After regexp escaping \\\\ = 1 slash, \\\\\\\\ = 2 slashes.

            // Also, the second args of replaceAll are neither strings nor regexps, and
            // are instead a special DSL used by Matcher. Therefore, we need to double
            // escape slashes (4 slashes) and quotes (3 slashes + ") in those strings.
            // Since we need to write \\ and \", we end up with 8 and 7 slashes,
            // respectively.
            // return in.ReplaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\\\"");
        }

        public static StringBuilder Join(StringBuilder sb, string sep, IConstructorArg[] types) 
        {
            if (types.Length > 0) 
            {
                sb.Append(types[0].GetType());
                for (int i = 1; i < types.Length; i++) 
                {
                    sb.Append(sep).Append(types[i].GetType());
                }
            }
            return sb;
        }
        #endregion text file serialization
    }
}