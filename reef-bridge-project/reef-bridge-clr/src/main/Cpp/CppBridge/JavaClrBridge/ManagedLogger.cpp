#include "Clr2JavaImpl.h"

using namespace JavaClrBridge;

namespace Microsoft
{
	namespace Reef
	{
		namespace Driver
		{
			namespace Bridge
			{				
				ManagedLogger::ManagedLogger(String^ className)
				{
					_logger = BridgeLogger::GetLogger(className);	
				}
				BridgeLogger^  ManagedLogger::GetLogger(String^ className)
				{
					if(_logger == nullptr)
					{
						_logger = BridgeLogger::GetLogger(className);
					}
					return _logger;
				}

			}
		}
	}
}