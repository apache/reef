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
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Interface;
using Com.Microsoft.Tang.Util;

namespace Com.Microsoft.Tang.Implementations
{
    public class ParameterParser
    {
        MonotonicTreeMap<String, ConstructorInfo> parsers = new MonotonicTreeMap<String, ConstructorInfo>();

        public void AddParser(Type ec)
        {
            //Class<?> tc = (Class<?>)ReflectionUtilities.getInterfaceTarget(ExternalConstructor.class, ec);
            //addParser((Class)tc, (Class)ec);
        }

    
        public object Parse(Type c, String s) 
        {
            Type d = ReflectionUtilities.BoxClass(c);
            foreach (Type e in ReflectionUtilities.ClassAndAncestors(d)) 
            {
                string name = e.FullName;
                if (parsers.ContainsKey(name)) 
                {
                    object ret = Parse(name, s);
                    if (c.IsAssignableFrom(ret.GetType())) 
                    {
                        return ret;
                    } 
                    else 
                    {
                        throw new InvalidCastException("Cannot cast from " + ret.GetType() + " to " + c);
                    }
                }
            }
            return Parse(d.Name, s);
        }

        public object Parse(string name, string value) 
        {
            if (parsers.ContainsKey(name)) 
            {
                try 
                {
                    ConstructorInfo c = null;
                    parsers.TryGetValue(name, out c);
                    return ((IExternalConstructor)c.Invoke(new object[] { value })).NewInstance();
                }
                catch (TargetInvocationException e) 
                {
                   throw new ArgumentException("Error invoking constructor for "
                            + name, e);
                }
            } 
            else if (name.Equals(typeof(string).Name)) 
            {
                return (object) value;
            }
            if (name.Equals(typeof(Byte).Name)) 
            {
                return (object) (Byte) Byte.Parse(value);
            }
            if (name.Equals(typeof(Char).Name)) {
                return (object) (Char) value[0];
            }
            if (name.Equals(typeof(short).Name))
            {
                return (System.Int16)System.Int16.Parse(value);
            }
            if (name.Equals(typeof(int).Name))
            {
                return (object) (Int32) Int32.Parse(value);
            }
            if (name.Equals(typeof(long).Name))
            {
                return (object)(Int64)Int64.Parse(value);
            }
            if (name.Equals(typeof(float).Name))
            {
                return (object)(Single)Single.Parse(value);
            }
            if (name.Equals(typeof(Double).Name) )
            {
                return (object) (Double) Double.Parse(value);
            }
            if (name.Equals(typeof(Boolean).Name)) 
            {
                return (object) (Boolean) Boolean.Parse(value);
            }
            if (name.Equals(typeof(byte[]).Name))  //TO verify
            {
                byte[] bytes = new byte[value.Length * sizeof(char)];
                System.Buffer.BlockCopy(value.ToCharArray(), 0, bytes, 0, bytes.Length);
                return bytes;
            }
            //if (name.Equals(Void.class.getName())) {
            //    throw new ClassCastException("Can't instantiate void");
            //}
            throw new NotSupportedException("Don't know how to parse a " + name);
       }
    }
}
