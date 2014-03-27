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
				_jobjectDriverManager = jobjectDriverManager;
				_jclassDriverManager = env->GetObjectClass (_jobjectDriverManager);

			}
			void Clr2JavaImpl::Emanager_submit(array<byte>^ bytes)
			{				
				try
				{
					jbyteArray ret = JavaByteArrayFromManagedByteArray(_env, bytes);				
					byte * pb = (byte*)_env->GetByteArrayElements (ret, NULL);
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

			void Clr2JavaImpl::AllocatedEvaluatorSubmitContextAndTask(String^ contextConfigStr, String^ taskConfigStr)
			{
				Console::WriteLine("AllocatedEvaluatorSubmitContextAndTask not implemented for Clr2JavaImpl");
			}
		}
	}
}