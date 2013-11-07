using System;
using Com.Microsoft.Tang.Annotations;

namespace Com.Microsoft.Tang.Examples
{
    public class Timer
    {
        [NamedParameter("Number of seconds to sleep", "sec", "10")]
        class Seconds : Name<Int32> { }
        private readonly int seconds;

        [Inject]
        public Timer([Parameter(Value = typeof(Seconds))] int seconds)
        {
            if (seconds < 0)
            {
                throw new ArgumentException("Cannot sleep for negative time!");
            }
            this.seconds = seconds;
        }
    }

    public interface  A
    {

    }

    public class B : A
    {
        public class B1 {
            public class B2 {}
        }
    }

    public class C : B
    {
    }

    public static class E
    {
    }

}
