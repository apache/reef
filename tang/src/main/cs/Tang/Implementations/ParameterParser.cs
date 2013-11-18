using System;
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
            //if (name.Equals(typeof(Short).Name)) 
            //{
            //    return (T) (Short) Short.parseShort(value);
            //}
            if (name.Equals(typeof(int).Name))
            {
                return (object) (Int32) Int32.Parse(value);
            }
            //if (name.Equals(Long.class.getName())) {
            //    return (T) (Long) Long.parseLong(value);
            //}
            //if (name.Equals(typeof(Float).Name)) 
            //{
            //    return (object) (Float) Float.parseFloat(value);
            //}
            if (name.Equals(typeof(Double).Name) )
            {
                return (object) (Double) Double.Parse(value);
            }
            if (name.Equals(typeof(Boolean).Name)) 
            {
                return (object) (Boolean) Boolean.Parse(value);
            }
            //if (name.Equals(Void.class.getName())) {
            //    throw new ClassCastException("Can't instantiate void");
            //}
            throw new NotSupportedException("Don't know how to parse a " + name);
       }
    }
}
