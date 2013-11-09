using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Annotations
{
    /// <summary>
    /// DefaultImplementationAttribute
    /// </summary>
    class DefaultImplementationAttribute : System.Attribute
    {
        public Type Value { get; set; }
        public string Name { get; set; } 
    }
}
