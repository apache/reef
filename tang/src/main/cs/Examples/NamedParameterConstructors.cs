using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;

namespace Com.Microsoft.Tang.Examples
{
    public class NamedParameterConstructors
    {
        [NamedParameter()]
        public class X : Name<String> 
        {
        };

        [Inject]
        public NamedParameterConstructors(String x, [Parameter(Value = typeof(X))] String y) 
        {
        }
    }
}
