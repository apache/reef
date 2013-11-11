using System;
using Com.Microsoft.Tang.Annotations;

namespace Com.Microsoft.Tang.Examples
{
    public class SimpleConstructors 
    {
        [Inject]
        public SimpleConstructors() 
        {
        }

        [Inject]
        public SimpleConstructors(int x) 
        {
        }

        [Inject]
        public SimpleConstructors(int x, String y) 
        {
        }

    }
}
