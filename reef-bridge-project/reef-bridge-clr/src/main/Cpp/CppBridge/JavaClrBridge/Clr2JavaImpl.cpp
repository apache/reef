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

			void Clr2JavaImpl::AllocatedEvaluatorSubmitContextAndTask(String^ contextConfigStr, String^ taskConfigStr)
			{
				Console::WriteLine("AllocatedEvaluatorSubmitContextAndTask not implemented for Clr2JavaImpl");
			}
		}
	}
}