#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Driver
		{
			namespace Bridge
			{
				ref class ManagedLog
				{
				internal:
					static BridgeLogger^ LOGGER = BridgeLogger::GetLogger("<C++>");
				};
				
				EvaluatorRequestorClr2Java::EvaluatorRequestorClr2Java(JNIEnv *env, jobject jevaluatorRequestor)
				{
					ManagedLog::LOGGER->LogStart("EvaluatorRequestorClr2Java::EvaluatorRequestorClr2Java");
					pin_ptr<JavaVM*> pJavaVm = &_jvm;
					int gotVm = env -> GetJavaVM(pJavaVm);
					_jobjectEvaluatorRequestor = reinterpret_cast<jobject>(env->NewGlobalRef(jevaluatorRequestor));
					ManagedLog::LOGGER->LogStop("EvaluatorRequestorClr2Java::EvaluatorRequestorClr2Java");
				}

				void EvaluatorRequestorClr2Java::Submit(IEvaluatorRequest^ request)
				{
					ManagedLog::LOGGER->LogStart("EvaluatorRequestorClr2Java::Submit");
					JNIEnv *env = RetrieveEnv(_jvm);
					jclass jclassEvaluatorRequestor = env->GetObjectClass (_jobjectEvaluatorRequestor);
					jmethodID jmidSubmit = env->GetMethodID(jclassEvaluatorRequestor, "submit", "(IILjava/lang/String;)V");

					if(jmidSubmit == NULL)
					{
						fprintf(stdout, " jmidSubmit is NULL\n"); fflush (stdout);
						return;
					}
					env -> CallObjectMethod(
						_jobjectEvaluatorRequestor, 
						jmidSubmit, 
						request -> Number,
						request -> MemoryMegaBytes,
						JavaStringFromManagedString(env, request -> Rack));
					ManagedLog::LOGGER->LogStop("EvaluatorRequestorClr2Java::Submit");
				}

				void EvaluatorRequestorClr2Java::OnError(String^ message)
				{
					ManagedLog::LOGGER->Log("EvaluatorRequestorClr2Java::OnError");									
					JNIEnv *env = RetrieveEnv(_jvm);	
					HandleClr2JavaError(env, message, _jobjectEvaluatorRequestor);
				}
			}
		}
	}
}