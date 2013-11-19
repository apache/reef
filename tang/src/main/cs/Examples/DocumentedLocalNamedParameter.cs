using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;

namespace Com.Microsoft.Tang.Examples
{
    public class DocumentedLocalNamedParameter
    {
        [NamedParameter(Documentation = "doc stuff", ShortName = "DocFoo", DefaultValue = "some value")]
        sealed class Foo : Name<String> 
        {
        }

        [Inject]
        public DocumentedLocalNamedParameter([Parameter(Value = typeof(Foo))] String s) 
        {
        }
    }
}
