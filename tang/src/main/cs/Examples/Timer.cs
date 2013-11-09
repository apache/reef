using System;
using System.Threading;
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

        public void sleep()  
        {
            Thread.Sleep(seconds * 1000);
        }
    }
}
