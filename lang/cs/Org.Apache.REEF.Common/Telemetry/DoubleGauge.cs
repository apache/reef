using System;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Telemetry
{
    class DoubleGauge : MetricBase<double>
    {
        public override bool IsImmutable
        {
            get { return true; }
        }

        public DoubleGauge(string name, string description)
            : base(name, description)
        {
        }

        [JsonConstructor]
        internal DoubleGauge(string name, string description, long timeStamp, double value)
            : base(name, description, timeStamp, value)
        {
        }

        public override IMetric CreateInstanceWithNewValue(object val)
        {
            return new DoubleGauge(Name, Description, DateTime.Now.Ticks, (double)val);
        }
    }
}
