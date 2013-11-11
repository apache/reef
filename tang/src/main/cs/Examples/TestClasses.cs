using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;

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
        string s;
        int v;

        [Inject]
        public C(string s, int v)
        {
            this.s = s;
            this.v = v;
        }
    }

    public static class E
    {
    }
}
