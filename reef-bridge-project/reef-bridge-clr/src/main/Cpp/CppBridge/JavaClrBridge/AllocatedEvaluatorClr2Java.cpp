#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Driver
		{
			namespace Bridge
			{				
				AllocatedEvaluatorClr2Java::AllocatedEvaluatorClr2Java(JNIEnv *env, jobject jallocatedEvaluator)
				{
					pin_ptr<JavaVM*> pJavaVm = &_jvm;
					int gotVm = env -> GetJavaVM(pJavaVm);
					_jobjectAllocatedEvaluator = reinterpret_cast<jobject>(env->NewGlobalRef(jallocatedEvaluator));
					
					fprintf(stdout, "AllocatedEvaluatorClr2Java env %p\n", env); fflush (stdout);
					fprintf(stdout, "AllocatedEvaluatorClr2Java _jvm %p\n", _jvm); fflush (stdout);
					fprintf(stdout, "AllocatedEvaluatorClr2Java _jobjectAllocatedEvaluator %p\n", _jobjectAllocatedEvaluator); fflush (stdout);	

					jclass jclassAllocatedEvaluator = env->GetObjectClass (_jobjectAllocatedEvaluator);
					jfieldID jidEvaluatorId = env->GetFieldID(jclassAllocatedEvaluator, "evaluatorId", "Ljava/lang/String;");
					_jstringId = (jstring)env->GetObjectField(_jobjectAllocatedEvaluator, jidEvaluatorId);
					_jstringId = reinterpret_cast<jstring>(env->NewGlobalRef(_jstringId));

					jfieldID jidNameServerInfo = env->GetFieldID(jclassAllocatedEvaluator, "nameServerInfo", "Ljava/lang/String;");
					_jstringNameServerInfo = (jstring)env->GetObjectField(_jobjectAllocatedEvaluator, jidNameServerInfo);
					_jstringNameServerInfo = reinterpret_cast<jstring>(env->NewGlobalRef(_jstringNameServerInfo));
				}

				void AllocatedEvaluatorClr2Java::SubmitContext(String^ contextConfigStr)
				{
					fprintf(stdout, "AllocatedEvaluatorClr2Java::SubmitContext"); fflush (stdout);										
					JNIEnv *env = RetrieveEnv(_jvm);
					jclass jclassAllocatedEvaluator = env->GetObjectClass (_jobjectAllocatedEvaluator);
					jmethodID jmidSubmitContext = env->GetMethodID(jclassAllocatedEvaluator, "submitContextString", "(Ljava/lang/String;)V");

					fprintf(stdout, "AllocatedEvaluatorClr2Java jclassAllocatedEvaluator %p\n", jclassAllocatedEvaluator); fflush (stdout);
					fprintf(stdout, "AllocatedEvaluatorClr2Java jmidSubmitContext %p\n", jmidSubmitContext); fflush (stdout);

					if(jmidSubmitContext == NULL)
					{
						fprintf(stdout, " jmidSubmitContext is NULL\n"); fflush (stdout);
						return;
					}
					env -> CallObjectMethod(
						_jobjectAllocatedEvaluator, 
						jmidSubmitContext, 
						JavaStringFromManagedString(env, contextConfigStr));
				}

				void AllocatedEvaluatorClr2Java::SubmitContextAndTask(String^ contextConfigStr, String^ taskConfigStr)
				{
					fprintf(stdout, "AllocatedEvaluatorClr2Java::SubmitContextAndTask"); fflush (stdout);										
					JNIEnv *env = RetrieveEnv(_jvm);	
					jclass jclassAllocatedEvaluator = env->GetObjectClass (_jobjectAllocatedEvaluator);
					jmethodID jmidSubmitContextAndTask = env->GetMethodID(jclassAllocatedEvaluator, "submitContextAndTaskString", "(Ljava/lang/String;Ljava/lang/String;)V");

					fprintf(stdout, "AllocatedEvaluatorClr2Java jclassAllocatedEvaluator %p\n", jclassAllocatedEvaluator); fflush (stdout);
					fprintf(stdout, "AllocatedEvaluatorClr2Java jmidSubmitContextAndTask %p\n", jmidSubmitContextAndTask); fflush (stdout);

					if(jmidSubmitContextAndTask == NULL)
					{
						fprintf(stdout, " jmidSubmitContextAndTask is NULL\n"); fflush (stdout);
						return;
					}
					env -> CallObjectMethod(
						_jobjectAllocatedEvaluator, 
						jmidSubmitContextAndTask, 
						JavaStringFromManagedString(env, contextConfigStr), 
						JavaStringFromManagedString(env, taskConfigStr));
				}

				void AllocatedEvaluatorClr2Java::SubmitContextAndService(String^ contextConfigStr, String^ serviceConfigStr)
				{
					fprintf(stdout, "AllocatedEvaluatorClr2Java::SubmitContextAndService"); fflush (stdout);										
					JNIEnv *env = RetrieveEnv(_jvm);	
					jclass jclassAllocatedEvaluator = env->GetObjectClass (_jobjectAllocatedEvaluator);
					jmethodID jmidSubmitContextAndService = env->GetMethodID(jclassAllocatedEvaluator, "submitContextAndServiceString", "(Ljava/lang/String;Ljava/lang/String;)V");	

					fprintf(stdout, "AllocatedEvaluatorClr2Java jclassAllocatedEvaluator %p\n", jclassAllocatedEvaluator); fflush (stdout);
					fprintf(stdout, "AllocatedEvaluatorClr2Java jmidSubmitContextAndService %p\n", jmidSubmitContextAndService); fflush (stdout);

					if(jmidSubmitContextAndService == NULL)
					{
						fprintf(stdout, " jmidSubmitContextAndService is NULL\n"); fflush (stdout);
						return;
					}
					env -> CallObjectMethod(
						_jobjectAllocatedEvaluator, 
						jmidSubmitContextAndService, 
						JavaStringFromManagedString(env, contextConfigStr), 
						JavaStringFromManagedString(env, serviceConfigStr));
				}

				void AllocatedEvaluatorClr2Java::SubmitContextAndServiceAndTask(String^ contextConfigStr, String^ serviceConfigStr, String^ taskConfigStr)
				{
					fprintf(stdout, "AllocatedEvaluatorClr2Java::SubmitContextAndServiceAndTask"); fflush (stdout);										
					JNIEnv *env = RetrieveEnv(_jvm);	
					jclass jclassAllocatedEvaluator = env->GetObjectClass (_jobjectAllocatedEvaluator);
					jmethodID jmidSubmitContextAndServiceAndTask = env->GetMethodID(jclassAllocatedEvaluator, "submitContextAndServiceAndTaskString", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");	

					fprintf(stdout, "AllocatedEvaluatorClr2Java jclassAllocatedEvaluator %p\n", jclassAllocatedEvaluator); fflush (stdout);
					fprintf(stdout, "AllocatedEvaluatorClr2Java jmidSubmitContextAndServiceAndTask %p\n", jmidSubmitContextAndServiceAndTask); fflush (stdout);

					if(jmidSubmitContextAndServiceAndTask == NULL)
					{
						fprintf(stdout, " jmidSubmitContextAndServiceAndTask is NULL\n"); fflush (stdout);
						return;
					}
					env -> CallObjectMethod(
						_jobjectAllocatedEvaluator, 
						jmidSubmitContextAndServiceAndTask, 
						JavaStringFromManagedString(env, contextConfigStr), 
						JavaStringFromManagedString(env, serviceConfigStr),
						JavaStringFromManagedString(env, taskConfigStr));
				}

				void AllocatedEvaluatorClr2Java::Close()
				{
					fprintf(stdout, "AllocatedEvaluatorClr2Java::Close"); fflush (stdout);					
					JNIEnv *env = RetrieveEnv(_jvm);	
					jclass jclassAllocatedEvaluator = env->GetObjectClass (_jobjectAllocatedEvaluator);
					jmethodID jmidClose = env->GetMethodID(jclassAllocatedEvaluator, "close", "()V");

					fprintf(stdout, "AllocatedEvaluatorClr2Java jclassAllocatedEvaluator %p\n", jclassAllocatedEvaluator); fflush (stdout);
					fprintf(stdout, "AllocatedEvaluatorClr2Java jmidClose %p\n", jmidClose); fflush (stdout);

					if(jmidClose == NULL)
					{
						fprintf(stdout, " jmidClose is NULL\n"); fflush (stdout);
						return;
					}
					env -> CallObjectMethod(
						_jobjectAllocatedEvaluator, 
						jmidClose);
				}

				String^ AllocatedEvaluatorClr2Java::GetId()
				{
					fprintf(stdout, "AllocatedEvaluatorClr2Java::GetId\n"); fflush (stdout);															
					
					JNIEnv *env = RetrieveEnv(_jvm);
					return ManagedStringFromJavaString(env, _jstringId);
				}

				String^ AllocatedEvaluatorClr2Java::GetNameServerInfo()
				{
					fprintf(stdout, "AllocatedEvaluatorClr2Java::GetNameServerInfo\n"); fflush (stdout);															
					
					JNIEnv *env = RetrieveEnv(_jvm);
					return ManagedStringFromJavaString(env, _jstringNameServerInfo);
				}

				IEvaluatorDescriptor^ AllocatedEvaluatorClr2Java::GetEvaluatorDescriptor()
				{
					fprintf(stdout, "AllocatedEvaluatorClr2Java::GetEvaluatorDescriptor"); fflush (stdout);															
					JNIEnv *env = RetrieveEnv(_jvm);	
					jclass jclassAllocatedEvaluator = env->GetObjectClass (_jobjectAllocatedEvaluator);
					jmethodID jmidGetEvaluatorDescriptor= env->GetMethodID(jclassAllocatedEvaluator, "getEvaluatorDescriptorSring", "()Ljava/lang/String;");	

					fprintf(stdout, "AllocatedEvaluatorClr2Java jclassAllocatedEvaluator %p\n", jclassAllocatedEvaluator); fflush (stdout);
					fprintf(stdout, "AllocatedEvaluatorClr2Java jmidGetEvaluatorDescriptor %p\n", jmidGetEvaluatorDescriptor); fflush (stdout);

					if(jmidGetEvaluatorDescriptor == NULL)
					{
						fprintf(stdout, " jmidGetEvaluatorDescriptor is NULL\n"); fflush (stdout);
						return nullptr;
					}
					jstring jevaluatorDescriptorString = (jstring)env -> CallObjectMethod(
						_jobjectAllocatedEvaluator, 
						jmidGetEvaluatorDescriptor);
					String^ evaluatorDescriptorString = ManagedStringFromJavaString(env, jevaluatorDescriptorString);
					EvaluatorDescriptorImpl^ descriptor = gcnew EvaluatorDescriptorImpl(evaluatorDescriptorString);
					return descriptor;
				}
			}
		}
	}
}