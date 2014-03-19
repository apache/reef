#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Interop
		{
			Clr2JavaImpl::Clr2JavaImpl (JNIEnv* env, jobject  jobjectEManager, jobject  jobjectDriverManager)
			{
				_env = env;
				_jobjectEManager = jobjectEManager;
				_jclassEManager =  env->GetObjectClass (_jobjectEManager);
				_jmidSubmit = env->GetMethodID(_jclassEManager, "submit", "([B)V");	
				//fprintf(stdout, "Emanager_submit _env %p\n", _env); fflush (stdout);
				//fprintf(stdout, "Emanager_submit _jobjectEManager %p\n", _jobjectEManager); fflush (stdout);
				//fprintf(stdout, "Emanager_submit _jclassEManager %p\n", _jclassEManager); fflush (stdout);
				//fprintf(stdout, "Emanager_submit _jmidSubmit %p\n", _jmidSubmit); fflush (stdout);
				_jobjectDriverManager = jobjectDriverManager;
				_jclassDriverManager = env->GetObjectClass (_jobjectDriverManager);

			}
			void Clr2JavaImpl::Emanager_submit(array<byte>^ bytes)
			{				
				try
				{
				_jclassEManager =  _env->GetObjectClass (_jobjectEManager);
				jbyteArray ret = JavaByteArrayFromManagedByteArray(_env, bytes);				
				byte * pb = (byte*)_env->GetByteArrayElements (ret, NULL);
				for (int i=0; i<bytes->Length; i++)
				{
					Console::WriteLine(pb[i]);
				}
				_env->CallObjectMethod (_jobjectEManager, _jmidSubmit, ret);
				_env->DeleteLocalRef(ret);
				
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