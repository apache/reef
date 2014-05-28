#include "InteropUtil.h"
#include "com_microsoft_reef_javabridge_NativeInterop.h"
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
				jobject		_jobjectInteropReturnInfo; 
				
				jmethodID	_jmidAddExceptionString;
				jmethodID	_jmidHasExceptions;
				jmethodID	_jmidsetReturnCode;
				jmethodID	_jmidgetReturnCode;

			public:
				InteropReturnInfo  (
					JNIEnv* env, 					
					jobject			jobjectInteropReturnInfo,
					ILogger^		logger
					);
				virtual void AddExceptionString(String^ exceptionString);       
				virtual Boolean HasExceptions();
				virtual void SetReturnCode(int rc);
				virtual int GetReturnCode();
			};
		}
	}
}