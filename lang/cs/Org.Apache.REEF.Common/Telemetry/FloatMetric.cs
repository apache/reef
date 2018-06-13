using System;
using System.Threading;
using Newtonsoft.Json;

namespace Org.Apache.REEF.Common.Telemetry
{
    public class FloatMetric : MetricBase<float>
    {
        public FloatMetric(string name, string description, bool isImmutable = true)
            : base(name, description, isImmutable)
        {
        }

        [JsonConstructor]
        public FloatMetric(string name, string description, float value)
            : base(name, description, value)
        {
        }

        public override void AssignNewValue(object val)
        {
            if (val.GetType() != _typedValue.GetType())
            {
                throw new ApplicationException("Cannot assign new value to metric because of type mismatch.");
            }
            Interlocked.Exchange(ref _typedValue, (float)val);
            _tracker.Track(val);
        }
    }
}
