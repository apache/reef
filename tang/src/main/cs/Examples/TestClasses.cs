using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Examples
{
    public interface A
    {

    }

    public class B : A
    {
        public class B1
        {
            public class B2 { }
        }
    }

    public class C : B
    {
    }

    public static class E
    {
    }
}
