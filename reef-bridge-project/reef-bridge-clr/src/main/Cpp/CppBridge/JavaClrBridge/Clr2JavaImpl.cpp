#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Interop
		{
			Clr2JavaImpl::Clr2JavaImpl (JNIEnv* env, jobject  jobjectEManager)
			{
				_env = env;
				_jobjectEManager = jobjectEManager;
				_jclassEManager =  env->GetObjectClass (_jobjectEManager);
				_jmidSubmit = env->GetMethodID(_jclassEManager, "submit", "([B)V");	
			}
			void Clr2JavaImpl::Emanager_submit(array<byte>^ bytes)
			{				
				fprintf(stdout, "Emanager_submit _env %p\n", _env); fflush (stdout);
				fprintf(stdout, "Emanager_submit _jobjectEManager %p\n", _jobjectEManager); fflush (stdout);
				fprintf(stdout, "Emanager_submit _jclassEManager %p\n", _jclassEManager); fflush (stdout);
				fprintf(stdout, "Emanager_submit _jmidSubmit %p\n", _jmidSubmit); fflush (stdout);
				try
				{
				jmethodID jmidSubmit2 = _env->GetMethodID(_jclassEManager, "submit2", "()V");	
				fprintf(stdout, "Emanager_submit _jmidSubmit2 %p\n", jmidSubmit2); fflush (stdout);				
				_env->CallObjectMethod (_jclassEManager, jmidSubmit2);
				fprintf(stdout, "Emanager_submit after _jmidSubmit2 \n"); fflush (stdout);				
				jbyteArray ret = JavaByteArrayFromManagedByteArray(_env, bytes);
				fprintf(stdout, "Emanager_submit After JavaByteArrayFromManagedByteArray\n"); fflush (stdout);
				_env->CallObjectMethod (_jclassEManager, _jmidSubmit, ret);
					}
	catch (System::Exception^ ex)
	{
		Console::WriteLine("Exception in Java_javabridge_NativeInterop_CallClrSystemOnStartHandler");
		Console::WriteLine(ex->Message);
		Console::WriteLine(ex->StackTrace);
	}

			}
		}
	}
}