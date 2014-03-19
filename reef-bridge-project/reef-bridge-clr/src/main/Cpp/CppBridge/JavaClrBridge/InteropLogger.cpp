#include "InteropLogger.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Interop
		{
			InteropLogger::InteropLogger (JNIEnv* env, jobject jobjectInteropLogger)
			{
				_env = env;
				_jobjectInteropLogger = jobjectInteropLogger;
				_jclassInteropLogger = env->GetObjectClass(jobjectInteropLogger);
				wchar_t formatBuf[1024];
				if (NULL == _jclassInteropLogger)
				{
					swprintf_s (formatBuf, sizeof(formatBuf)/sizeof(wchar_t), L"_jclassInteropLogger %p\n", _jclassInteropLogger);  fwprintf (stdout, formatBuf); fflush (stdout);
				}
				_jmidLog  = env->GetMethodID(_jclassInteropLogger, "Log", "(ILjava/lang/String;)V");	
				if (NULL == _jmidLog)
				{				
					swprintf_s (formatBuf, sizeof(formatBuf)/sizeof(wchar_t), L"_jmidLog %p\n", _jmidLog);  fwprintf (stdout, formatBuf); fflush (stdout);
				}

			}
			void InteropLogger::Log(TraceLevel traceLevel, String^ message)
			{					
				pin_ptr<const wchar_t> wch = PtrToStringChars(message);		
				jstring msg = _env->NewString((const jchar*)wch, message->Length);				
				_env->CallObjectMethod(_jobjectInteropLogger, _jmidLog, (int)traceLevel, msg);
			}
		}
	}
}