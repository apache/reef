#include "InteropReturnInfo.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Interop
		{
			InteropReturnInfo::InteropReturnInfo (
				JNIEnv*			env, 
				jclass			jclassInteropReturnInfo, 
				jobject			jobjectInteropReturnInfo 
				//jmethodID		jmidAddExceptionString,
				//jmethodID		jmidHasExceptions,
				//jmethodID		jmidSetReturnCode,
				//jmethodID		jmidGetReturnCode
				)
			{
				_env = env;
				_jclassInteropReturnInfo = jclassInteropReturnInfo;
				_jobjectInteropReturnInfo = jobjectInteropReturnInfo;				
				//_jmidAddExceptionString = jmidAddExceptionString;
				//_jmidHasExceptions = jmidHasExceptions;
				//_jmidSetReturnCode = jmidSetReturnCode;
				//_jmidGetReturnCode = jmidGetReturnCode;
				// Get a class reference for this object
				jclass thisClass = env->GetObjectClass(jobjectInteropReturnInfo);
				fprintf(stdout, "jmid 00 %p\n", thisClass);  fflush(stdout);				
				_jmidAddExceptionString = env->GetMethodID(thisClass, "AddExceptionString", "(Ljava/lang/String;)V");	
				fprintf(stdout, "jmid 11 %p\n", _jmidAddExceptionString);
				fflush(stdout);
				_jmidHasExceptions = env->GetMethodID(thisClass, "hasExceptions", "()Z");	
				fprintf(stdout, "jmid 22 %p\n", _jmidHasExceptions); fflush(stdout);			
			}

			void InteropReturnInfo::AddExceptionString(String^ exceptionString)
			{
				pin_ptr<const wchar_t> wch = PtrToStringChars(exceptionString);
				jstring ret = _env->NewString((const jchar*)wch, exceptionString->Length);
				_env->CallObjectMethod(_jobjectInteropReturnInfo, _jmidAddExceptionString, ret);
				fprintf(stdout, "xx jmid 66\n"); fflush(stdout);
			}

			Boolean InteropReturnInfo::HasExceptions()
			{
				return false;
			}
			void InteropReturnInfo::SetReturnCode(int rc)
			{
			}
			int InteropReturnInfo::GetReturnCode()
			{
				return 0;
			}			
			
		}
	}
}