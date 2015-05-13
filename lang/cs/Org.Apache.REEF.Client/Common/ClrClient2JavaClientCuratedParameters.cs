using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote.Parameters;

namespace Org.Apache.REEF.Client.Common
{
    public class ClrClient2JavaClientCuratedParameters
    {
        public int TcpPortRangeStart { get; private set; }
        public int TcpPortRangeCount { get; private set; }
        public int TcpPortRangeTryCount { get; private set; }
        public int TcpPortRangeSeed { get; private set; }


        [Inject]
        private ClrClient2JavaClientCuratedParameters(
            [Parameter(typeof(TcpPortRangeStart))] int tcpPortRangeStart,
            [Parameter(typeof(TcpPortRangeCount))] int tcpPortRangeCount,
            [Parameter(typeof(TcpPortRangeTryCount))] int tcpPortRangeTryCount,
            [Parameter(typeof(TcpPortRangeSeed))] int tcpPortRangeSeed)
        {
            TcpPortRangeStart = tcpPortRangeStart;
            TcpPortRangeCount = tcpPortRangeCount;
            TcpPortRangeTryCount = tcpPortRangeTryCount;
            TcpPortRangeSeed = tcpPortRangeSeed;
        }
    }
}
