using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;

namespace Com.Microsoft.Tang.Examples
{
    public interface IActivity
    {
        byte[] Call(byte[] memento);
    }

    public sealed class HelloActivity : IActivity
    {
        [Inject]
        public HelloActivity()
        {
        }

        public byte[] Call(byte[] memento)
        {
            System.Console.WriteLine("Hello, REEF!");
            return null;
        }
    }

}
