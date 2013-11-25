using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Util
{
    public class ClassNameParser
    {
        public static string[] GetEnclosingClassFullNames(Type t)
        {
            return GetEnclosingClassFullNames(t.FullName);
        }

        public static string[] GetEnclosingClassFullNames(string name)
        {
            string[] path = name.Split('+');
            for (int i = 1; i < path.Length; i++)
            {
                path[i] = path[i - 1] + "+" + path[i];
            }
            return path;
        }

        public static string[] GetEnclosingClassShortNames(Type t)
        {
            return GetEnclosingClassShortNames(t.FullName);
        }

        public static string[] GetEnclosingClassShortNames(string fullName)
        {
            string[] path = fullName.Split('+');
            string sysName = ParseSystemName(fullName);

            if (path.Length > 1 || sysName == null)
            {
                string[] first = path[0].Split('.');
                path[0] = first[first.Length - 1];
            }
            else
            {
                path[0] = sysName;
            }

            return path;
        }

        public static string ParseSystemName(string name)
        {
            string[] token = name.Split('[');
            if (token.Length > 1) //system name System.IComparable`1[[System.String, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089]]
            {
                string[] prefixes = token[0].Split('.');
                return prefixes[prefixes.Length - 1];
            }
            return null;
        }
    }
}
