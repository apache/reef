using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;

namespace com.microsoft.reef.activity
{
    public sealed class HelloActivity : IActivity
    {
        [Inject]
        public HelloActivity()
        {
        }

        public byte[] Call(byte[] memento)
        {
            System.Console.WriteLine("Hello, REEF CLR!");
            return null;
        }
    }
}
