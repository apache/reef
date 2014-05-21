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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Exceptions;
using Com.Microsoft.Tang.Implementations;
using Com.Microsoft.Tang.Interface;
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.formats
{
    public class ConfigurationFile
    {
        public static void WriteConfigurationFile(IConfiguration c, string fileName) 
        {
            using (FileStream aFile = new FileStream(fileName, FileMode.OpenOrCreate))
            {
                using (StreamWriter sw = new StreamWriter(aFile))
                {
                    sw.Write(ToConfigurationString(c));
                }
            }
        }

        public static String ToConfigurationString(IConfiguration c) 
        {
            StringBuilder sb = new StringBuilder();
            foreach (string s in ToConfigurationStringList(c)) 
            {
                sb.Append(s);
                sb.Append('\n');
            }
            return sb.ToString();
        }

        public static List<String> ToConfigurationStringList(IConfiguration c) 
        {
            ConfigurationImpl conf = (ConfigurationImpl) c;
            List<string> l = new List<string>();
            foreach (IClassNode opt in conf.GetBoundImplementations()) 
            {
                l.Add(opt.GetFullName() + '=' + Escape(conf.GetBoundImplementation(opt).GetFullName()));
            }
            
            foreach (IClassNode opt in conf.GetBoundConstructors()) 
            {
                l.Add(opt.GetFullName() + '=' + Escape(conf.GetBoundConstructor(opt).GetFullName()));
            }
            foreach (INamedParameterNode opt in conf.GetNamedParameters()) 
            {
                l.Add(opt.GetFullName() + '=' + Escape(conf.GetNamedParameter(opt)));
            }
            //foreach (IClassNode cn in conf.GetLegacyConstructors()) 
            //{
            //    StringBuilder sb = new StringBuilder();
            //    Join(sb, "-", conf.GetLegacyConstructor(cn).GetArgs());
            //    l.Add(cn.GetFullName() + Escape('=' + ConfigurationBuilderImpl.INIT + '(' + sb.ToString() + ')'));
            //    //s.append(cn.getFullName()).append('=').append(ConfigurationBuilderImpl.INIT).append('(');
            //    //      .append(")\n");
            //}

            //foreach (Entry<NamedParameterNode<Set<?>>,Object> e in conf.GetBoundSets()) 
            //{
            //    final String val;
            //    if(e.getValue() instanceof String) {
            //        val = (String)e.getValue();
            //    } else if(e.getValue() instanceof Node) {
            //        val = ((Node)e.getValue()).getFullName();
            //    } else {
            //        throw new IllegalStateException();
            //    }
            //    l.add(e.getKey().getFullName() + '=' + escape(val));
            //    //      s.append(e.getKey().getFullName()).append('=').append(val).append("\n");
            //}

            return l;//s.toString();
        }

        public static void AddConfiguration(IConfigurationBuilder conf, string fileName)
        {
            IDictionary<string, string> settings = ReadFromFile(fileName);
            AddConfiguration(conf, settings);
        }

        public static void AddConfiguration(IConfigurationBuilder conf, IDictionary<string, string> settings)
        {
            foreach (KeyValuePair<string, string> kv in settings)
            {
                try
                {
                    conf.Bind(kv.Key, kv.Value);
                }
                catch (BindException e)
                {
                    throw new BindException("Failed to process configuration tuple: [" + kv.Key + "=" + kv.Value + "]", e);
                }
                catch (ClassHierarchyException e)
                {
                    throw new ClassHierarchyException("Failed to process configuration tuple: [" + kv.Key + "=" + kv.Value + "]", e);
                }
            }
        }

        private static IDictionary<string, string> ReadFromFile(string fileName)
        {
            IDictionary<string, string> settings = new Dictionary<string, string>();
            using (StreamReader sr = new StreamReader(fileName))
            {
                while (!sr.EndOfStream)
                {
                    string line = sr.ReadLine();
                    string[] p = line.Split('=');
                    settings.Add(p[0], p[1]);
                }
            }
            return settings;
        }

       /**
        * Replace any \'s in the input string with \\. and any "'s with \".
        * @param in
        * @return
        */

        private static string Escape(string str) 
        {
            return str;  //TODO
            // After regexp escaping \\\\ = 1 slash, \\\\\\\\ = 2 slashes.

            // Also, the second args of replaceAll are neither strings nor regexps, and
            // are instead a special DSL used by Matcher. Therefore, we need to double
            // escape slashes (4 slashes) and quotes (3 slashes + ") in those strings.
            // Since we need to write \\ and \", we end up with 8 and 7 slashes,
            // respectively.
            //return in.ReplaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\\\"");
        }

        private static StringBuilder Join(StringBuilder sb, String sep, IConstructorArg[] types) 
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


    }
}
