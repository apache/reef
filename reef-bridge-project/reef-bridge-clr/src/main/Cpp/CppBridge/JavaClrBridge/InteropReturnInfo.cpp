#include "InteropReturnInfo.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Interop
		{
			InteropReturnInfo::InteropReturnInfo (
				JNIEnv*			env, 				
				jobject			jobjectInteropReturnInfo,
				ILogger^			logger
				)
			{
				_env = env;				
				_jobjectInteropReturnInfo = jobjectInteropReturnInfo;				
				jclass thisClass = env->GetObjectClass(jobjectInteropReturnInfo);
				wchar_t formatBuf[1024];

				swprintf_s (formatBuf, sizeof(formatBuf)/sizeof(wchar_t), L"zzzzzzz this should be printed by java jmid 00 %p\n", thisClass);
				logger->Log(TraceLevel::Error, gcnew String(formatBuf));  
				_jmidAddExceptionString = env->GetMethodID(thisClass, "addExceptionString", "(Ljava/lang/String;)V");	
				if (NULL == _jmidAddExceptionString)
				{
					swprintf_s (formatBuf, sizeof(formatBuf)/sizeof(wchar_t), L"_jmidAddExceptionString %p\n", _jmidAddExceptionString);  fwprintf (stdout, formatBuf); fflush (stdout);
				}

				_jmidHasExceptions = env->GetMethodID(thisClass, "hasExceptions", "()Z");	
				if (NULL == _jmidHasExceptions)
				{
					swprintf_s (formatBuf, sizeof(formatBuf)/sizeof(wchar_t), L"_jmidHasExceptions %p\n", _jmidHasExceptions);  fwprintf (stdout, formatBuf); fflush (stdout);
				}
				
				_jmidsetReturnCode = env->GetMethodID(thisClass, "setReturnCode", "(I)V");	
				if (NULL == _jmidsetReturnCode)
				{
					swprintf_s (formatBuf, sizeof(formatBuf)/sizeof(wchar_t), L"_jmidsetReturnCode %p\n", _jmidsetReturnCode);  fwprintf (stdout, formatBuf); fflush (stdout);
				}

				_jmidgetReturnCode = env->GetMethodID(thisClass, "getReturnCode", "()I");	
				if (NULL == _jmidgetReturnCode)
				{
					swprintf_s (formatBuf, sizeof(formatBuf)/sizeof(wchar_t), L"_jmidgetReturnCode %p\n", _jmidgetReturnCode);  fwprintf (stdout, formatBuf); fflush (stdout);
				}


			}

			void InteropReturnInfo::AddExceptionString(String^ exceptionString)
			{			
				HasExceptions();
				pin_ptr<const wchar_t> wch = PtrToStringChars(exceptionString);
				jstring ret = _env->NewString((const jchar*)wch, exceptionString->Length);
				_env->CallObjectMethod(_jobjectInteropReturnInfo, _jmidAddExceptionString, ret);
				HasExceptions();
			}

			Boolean InteropReturnInfo::HasExceptions()
			{
				jobject obj =_env->CallObjectMethod(_jobjectInteropReturnInfo, _jmidHasExceptions);
				return ((int)obj) != 0;
			}
			void InteropReturnInfo::SetReturnCode(int rc)
			{
				_env->CallObjectMethod(_jobjectInteropReturnInfo, _jmidsetReturnCode, rc);
				GetReturnCode();
			}
			int InteropReturnInfo::GetReturnCode()
			{
				jobject obj =_env->CallObjectMethod(_jobjectInteropReturnInfo, _jmidgetReturnCode);
				return (int)obj;
			}			
			
		}
	}
}