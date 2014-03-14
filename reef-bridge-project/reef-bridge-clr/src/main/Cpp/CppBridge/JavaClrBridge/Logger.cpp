#include "Ilogger.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Interop
		{
			Logger::Logger (JNIEnv* env, jclass jerrInfo, jmethodID jmid)
			{
				_env = env;
				_jerrorInfo = jerrInfo;
				_jmid = jmid;
			}
			void Logger::Log(TraceLevel traceLevel, String^ message)
			{	
				pin_ptr<const wchar_t> wch = PtrToStringChars(message);		
				jstring ret = _env->NewString((const jchar*)wch, message->Length);
				_env->CallStaticVoidMethod (_jerrorInfo, _jmid, ret, traceLevel);
			}
		}
	}
}