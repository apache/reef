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

namespace Org.Apache.REEF.Tang.Implementations.ClassHierarchy
{
    public class ParameterParser
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ParameterParser));

        readonly MonotonicTreeMap<String, ConstructorInfo> parsers = new MonotonicTreeMap<String, ConstructorInfo>();

        // ec: ACons, tc: A
        public void AddParser(Type ec)
        {
            Type tc = (Type)ReflectionUtilities.GetInterfaceTarget(typeof(IExternalConstructor<>), ec);
            AddParser(tc, ec);
        }

        // TODO
        // public  <T, U extends T> void AddParser(Class<U> clazz, Class<? extends ExternalConstructor<T>> ec) throws BindException {
        // public void AddParser<T, U, V>(GenericType<U> clazz, GenericType<V> ec) 
        //    where U : T
        //    where V:  IExternalConstructor<T>
        // {

        // clazz: A, ec: ACons
        private void AddParser(Type clazz, Type ec)
        {
            ConstructorInfo c = ec.GetConstructor(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance, null, new Type[] { typeof(string) }, null);
            
            if (c == null)
            {
                var e = new BindException("Constructor " + ReflectionUtilities.GetAssemblyQualifiedName(ec) + "(String) does not exist!");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
            }

            // c.setAccessible(true); // set as public 
            parsers.Add(ReflectionUtilities.GetAssemblyQualifiedName(clazz), c);
        }


        public void MergeIn(ParameterParser p)
        {
            foreach (string s in p.parsers.Keys)
            {
                if (!parsers.ContainsKey(s))
                {
                    ConstructorInfo ci;
                    p.parsers.TryGetValue(s, out ci);
                    parsers.Add(s, ci);
                }
                else
                {
                    ConstructorInfo oldC;
                    ConstructorInfo newC;
                    parsers.TryGetValue(s, out oldC);
                    p.parsers.TryGetValue(s, out newC);
                    if (!oldC.Equals(newC))
                    {
                        var e = new ArgumentException(
                        "Conflict detected when merging parameter parsers! To parse " + s
                        + " I have a: " + ReflectionUtilities.GetAssemblyQualifiedName(oldC.DeclaringType)
                        + " the other instance has a: " + ReflectionUtilities.GetAssemblyQualifiedName(newC.DeclaringType));
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                    }
                }
            }
        }

        // (Integer, "3") return object of new Integer(3)
        public object Parse(Type c, String s)
        {
            Type d = ReflectionUtilities.BoxClass(c);
            foreach (Type e in ReflectionUtilities.ClassAndAncestors(d)) // get all the super classes of Integer for example
            {
                string name = ReflectionUtilities.GetAssemblyQualifiedName(e);
                if (parsers.ContainsKey(name))
                {
                    object ret = Parse(name, s);
                    if (c.IsAssignableFrom(ret.GetType())) // check if ret can be cast as c
                    {
                        return ret;
                    }
                    else
                    {
                        var ex = new InvalidCastException("Cannot cast from " + ret.GetType() + " to " + c);
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                    }
                }
            }
            return Parse(d.Name, s);
        }

        // name: "Integer", value: "12"
        public object Parse(string name, string value)
        {
            if (parsers.ContainsKey(name))
            {
                try
                {
                    ConstructorInfo c = null;
                    parsers.TryGetValue(name, out c);
                    var o = c.Invoke(new object[] { value });
                    var m = o.GetType().GetMethod("NewInstance", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
                    return m.Invoke(o, new object[] { });
                }
                catch (TargetInvocationException e)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ArgumentException("Error invoking constructor for " + name, e), LOGGER);
                }
            }
            else if (name.Equals(typeof(string).Name))
            {
                return (object)value;
            }
            if (name.Equals(typeof(Byte).Name))
            {
                return (object)(Byte)Byte.Parse(value);
            }
            if (name.Equals(typeof(Char).Name))
            {
                return (object)(Char)value[0];
            }
            if (name.Equals(typeof(short).Name))
            {
                return (System.Int16)System.Int16.Parse(value);
            }
            if (name.Equals(typeof(int).Name))
            {
                return (object)(Int32)Int32.Parse(value);
            }
            if (name.Equals(typeof(long).Name))
            {
                return (object)(Int64)Int64.Parse(value);
            }
            if (name.Equals(typeof(float).Name))
            {
                return (object)(Single)Single.Parse(value);
            }
            if (name.Equals(typeof(Double).Name))
            {
                return (object)(Double)Double.Parse(value);
            }
            if (name.Equals(typeof(Boolean).Name))
            {
                return (object)(Boolean)Boolean.Parse(value);
            }
            if (name.Equals(typeof(byte[]).Name))  
            {
                byte[] bytes = new byte[value.Length * sizeof(char)];
                System.Buffer.BlockCopy(value.ToCharArray(), 0, bytes, 0, bytes.Length);
                return bytes;
            }
            return null; 
        }

        private static readonly ISet<string> BUILTIN_NAMES = new HashSet<string>(new string[]
            {
                typeof(string).AssemblyQualifiedName, 
                typeof(Byte).AssemblyQualifiedName,
                typeof(char).AssemblyQualifiedName,
                typeof(short).AssemblyQualifiedName,
                typeof(Int32).AssemblyQualifiedName,
                typeof(long).AssemblyQualifiedName,
                typeof(float).AssemblyQualifiedName,
                typeof(double).AssemblyQualifiedName,
                typeof(bool).AssemblyQualifiedName
            }); 
        
        public bool CanParse(string name)
        {
            return parsers.ContainsKey(name) || BUILTIN_NAMES.Contains(name);
        }
    }
}
