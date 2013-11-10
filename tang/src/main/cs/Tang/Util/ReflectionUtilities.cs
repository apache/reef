using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Util
{
    public class ReflectionUtilities
    {
        public static Type GetRawClass(Type type)
        {
            return type;
            //if (type.IsClass)
            //{
            //    return type;
            //}
            //else if (type.IsGenericType)
            //{
            //    return type.GetGenericTypeDefinition();
            //}
            //else if (type.IsGenericParameter)
            //{
            //    return typeof(object);
            //}
            //else
            //{
            //    throw new ArgumentException("Can't getRawClass for " + type + " of unknown type " + type);
            //}
        }

        public static Type getInterfaceTarget(Type iface, Type type)
        {
            //TODO
            return type;
        }
    }
}
