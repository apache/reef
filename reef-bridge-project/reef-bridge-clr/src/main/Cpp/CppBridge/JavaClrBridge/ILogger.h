#include "InteropUtil.h"
#include "javabridge_NativeInterop.h"
#include "JavaClrBridge.h"
#include "InteropAssemblies.h"

//#define _MT 1
//#define _DLL 1

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
			public ref class Logger : public ILogger
			{
				jclass  _jerrorInfo;
				jmethodID _jmid;
				JNIEnv* _env;

			public:
				Logger (JNIEnv* env, jclass jerrInfo, jmethodID jmid);
				virtual void Log(TraceLevel traceLevel, String^ message );
			};
		}
	}
}