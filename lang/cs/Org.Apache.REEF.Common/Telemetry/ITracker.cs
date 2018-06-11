using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Apache.REEF.Common.Telemetry
{
    public interface ITracker
    {
        void Track(object value);
    }
}
