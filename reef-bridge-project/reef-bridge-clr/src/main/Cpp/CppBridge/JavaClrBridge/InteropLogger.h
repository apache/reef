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
			public ref class InteropLogger : public ILogger
			{
				jobject _jobjectInteropLogger;
				jclass  _jclassInteropLogger;				
				jmethodID _jmidLog;
				JNIEnv* _env;

			public:
				InteropLogger (JNIEnv* env, jobject jobjectInteropLogger);
				virtual void Log(TraceLevel traceLevel, String^ message );
			};
		}
	}
}