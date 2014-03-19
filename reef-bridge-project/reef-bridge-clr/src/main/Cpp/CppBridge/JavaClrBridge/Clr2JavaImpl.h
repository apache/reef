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
			public ref class Clr2JavaImpl : public IClr2Java
			{
				jclass  _jclassEManager;
				jobject  _jobjectEManager;
				jmethodID _jmidSubmit;
				JNIEnv* _env;

			public:
				Clr2JavaImpl (JNIEnv* env, jobject jobjectEManager);
				virtual void Emanager_submit(array<byte>^ bytes);
			};
		}
	}
}