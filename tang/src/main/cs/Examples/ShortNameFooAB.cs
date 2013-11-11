using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;

namespace Com.Microsoft.Tang.Examples
{
    [NamedParameter(ShortName = "foo")]
    class ShortNameFooA : Name<String> 
    {
    }

    //when same short name is used, exception would throw when building the class hierarchy
    [NamedParameter(ShortName = "fooB")]
    class ShortNameFooB : Name<Int32> 
    {
    }
}
