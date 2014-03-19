#include "InteropUtil.h"
#include "javabridge_NativeInterop.h"
#include "JavaClrBridge.h"
#include "InteropAssemblies.h"
#using "clrhandler.dll" 

using namespace System;
using namespace System::IO;
using namespace System::Collections::Generic;
using namespace System::Runtime::InteropServices;
using namespace System::Reflection;
using namespace Microsoft::Reef::Interop;

namespace Microsoft
{
	namespace Reef
	{
		namespace Interop
		{
			public ref class InteropReturnInfo : public IInteropReturnInfo
			{
				JNIEnv* _env;
				jclass		_jclassInteropReturnInfo;
				jobject		_jobjectInteropReturnInfo; 
				
				jmethodID	_jmidAddExceptionString;
				jmethodID	_jmidHasExceptions;
				jmethodID	_jmidSetReturnCode;
				jmethodID	_jmidGetReturnCode;

			public:
				InteropReturnInfo  (
					JNIEnv* env, 
					jclass			jclassInteropReturnInfo, 
					jobject			jobjectInteropReturnInfo
					//jmethodID		jmidAddExceptionString,
					//jmethodID		jmidHasExceptions,
					//jmethodID		jmidSetReturnCode,
					//jmethodID		jmidGetReturnCode
					);
				virtual void AddExceptionString(String^ exceptionString);       
				virtual Boolean HasExceptions();
				virtual void SetReturnCode(int rc);
				virtual int GetReturnCode();
			};
		}
	}
}