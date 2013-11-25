using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Util
{
    public class AssemblyLoader
    {
        public IList<Assembly> Assemblies { get; set; }

        public AssemblyLoader(string[] files)
        {
            Assemblies = new List<Assembly>();
            foreach (var a in files)
            {
                Assemblies.Add(Assembly.LoadFrom(a));
            }
        }

        public Type GetType(string name)
        {
            Type t = Type.GetType(name);
            if (t == null)
            {
                foreach (var a in Assemblies)
                {
                    t = a.GetType(name);
                    if (t != null)
                    {
                        return t;
                    }
                }
            }
            return t;
        }
    }
}
