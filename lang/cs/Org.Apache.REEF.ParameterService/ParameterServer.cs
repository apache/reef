using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.ParameterService
{
    [NamedParameter("The id to use to register with the Name Service")]
    public class ParameterServerId : Name<string> { }


    internal class ParameterServer : IDisposable
    {
        private readonly string _serverId;
        private readonly INameClient _nameClient;

        [Inject]
        internal ParameterServer([Parameter(typeof(ParameterServerId))] string serverId,
            INameClient nameClient,
            ITcpPortProvider tcpPortProvider)
        {
            _serverId = serverId;
            _nameClient = nameClient;
            var ipPort = StartServer(tcpPortProvider);
            _nameClient.Register(serverId, new IPEndPoint(ipPort.Item1, ipPort.Item2));
        }

        private Tuple<IPAddress, int> StartServer(ITcpPortProvider tcpPortProvider)
        {
            //Instead of this start the actual parameter server
            //Get the ip & port it is listerning
            //Register that ip & port
            return new Tuple<IPAddress, int>(IPAddress.Loopback, tcpPortProvider.First());
        }

        public void Dispose()
        {
            //Free resources
            _nameClient.Unregister(_serverId);
            //throw new NotImplementedException();
        }
    }
}
