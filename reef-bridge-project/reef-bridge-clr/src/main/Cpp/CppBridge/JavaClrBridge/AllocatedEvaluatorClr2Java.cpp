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
					_env = env;
					_jobjectAllocatedEvaluator = jallocatedEvaluator;
					_jclassAllocatedEvaluator = _env->GetObjectClass (_jobjectAllocatedEvaluator);
					_jmidSubmitContextAndTask = _env->GetMethodID(_jclassAllocatedEvaluator, "submitContextAndTaskString", "(Ljava/lang/String;Ljava/lang/String;)V");	
					_jmidSubmitContext = _env->GetMethodID(_jclassAllocatedEvaluator, "submitContextString", "(Ljava/lang/String;)V");
					_jmidSubmitContextAndService = _env->GetMethodID(_jclassAllocatedEvaluator, "submitContextAndServiceString", "(Ljava/lang/String;Ljava/lang/String;)V");	
					_jmidSubmitContextAndServiceAndTask = _env->GetMethodID(_jclassAllocatedEvaluator, "submitContextAndServiceAndTaskString", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");	
					_jmidGetEvaluatorDescriptor= _env->GetMethodID(_jclassAllocatedEvaluator, "getEvaluatorDescriptorSring", "()Ljava/lang/String;");	

					fprintf(stdout, "AllocatedEvaluatorClr2Java _env %p\n", _env); fflush (stdout);
					fprintf(stdout, "AllocatedEvaluatorClr2Java _jclassAllocatedEvaluator %p\n", _jclassAllocatedEvaluator); fflush (stdout);
					fprintf(stdout, "AllocatedEvaluatorClr2Java _jobjectAllocatedEvaluator %p\n", _jobjectAllocatedEvaluator); fflush (stdout);
					fprintf(stdout, "AllocatedEvaluatorClr2Java _jmidSubmitContextAndTask %p\n", _jmidSubmitContextAndTask); fflush (stdout);
					fprintf(stdout, "AllocatedEvaluatorClr2Java _jmidSubmitContext %p\n", _jmidSubmitContextAndTask); fflush (stdout);
					fprintf(stdout, "AllocatedEvaluatorClr2Java _jmidSubmitContextAndService %p\n", _jmidSubmitContextAndService); fflush (stdout);
					fprintf(stdout, "AllocatedEvaluatorClr2Java _jmidSubmitContextAndServiceAndTask %p\n", _jmidSubmitContextAndServiceAndTask); fflush (stdout);
					fprintf(stdout, "AllocatedEvaluatorClr2Java _jmidGetEvaluatorDescriptor %p\n", _jmidGetEvaluatorDescriptor); fflush (stdout);					
				}

				void AllocatedEvaluatorClr2Java::SubmitContextAndTask(String^ contextConfigStr, String^ taskConfigStr)
				{
					if(_jobjectAllocatedEvaluator == NULL)
					{
						fprintf(stdout, " _jobjectAllocatedEvaluator is NULL\n"); fflush (stdout);
						return;
					}
					if(_jmidSubmitContextAndTask == NULL)
					{
						fprintf(stdout, " _jmidSubmitContextAndTask is NULL\n"); fflush (stdout);
						return;
					}
					_env -> CallObjectMethod(
						_jobjectAllocatedEvaluator, 
						_jmidSubmitContextAndTask, 
						JavaStringFromManagedString(_env, contextConfigStr), 
						JavaStringFromManagedString(_env, taskConfigStr));
				}

				void AllocatedEvaluatorClr2Java::SubmitContext(String^ contextConfigStr)
				{
					if(_jobjectAllocatedEvaluator == NULL)
					{
						fprintf(stdout, " _jobjectAllocatedEvaluator is NULL\n"); fflush (stdout);
						return;
					}
					if(_jmidSubmitContext == NULL)
					{
						fprintf(stdout, " _jmidSubmitContext is NULL\n"); fflush (stdout);
						return;
					}
					_env -> CallObjectMethod(
						_jobjectAllocatedEvaluator, 
						_jmidSubmitContext, 
						JavaStringFromManagedString(_env, contextConfigStr));
				}

				void AllocatedEvaluatorClr2Java::SubmitContextAndService(String^ contextConfigStr, String^ serviceConfigStr)
				{
					if(_jobjectAllocatedEvaluator == NULL)
					{
						fprintf(stdout, " _jobjectAllocatedEvaluator is NULL\n"); fflush (stdout);
						return;
					}
					if(_jmidSubmitContextAndService == NULL)
					{
						fprintf(stdout, " _jmidSubmitContextAndService is NULL\n"); fflush (stdout);
						return;
					}
					_env -> CallObjectMethod(
						_jobjectAllocatedEvaluator, 
						_jmidSubmitContextAndService, 
						JavaStringFromManagedString(_env, contextConfigStr), 
						JavaStringFromManagedString(_env, serviceConfigStr));
				}

				void AllocatedEvaluatorClr2Java::SubmitContextAndServiceAndTask(String^ contextConfigStr, String^ serviceConfigStr, String^ taskConfigStr)
				{
					if(_jobjectAllocatedEvaluator == NULL)
					{
						fprintf(stdout, " _jobjectAllocatedEvaluator is NULL\n"); fflush (stdout);
						return;
					}
					if(_jmidSubmitContextAndServiceAndTask == NULL)
					{
						fprintf(stdout, " _jmidSubmitContextAndServiceAndTask is NULL\n"); fflush (stdout);
						return;
					}
					_env -> CallObjectMethod(
						_jobjectAllocatedEvaluator, 
						_jmidSubmitContextAndServiceAndTask, 
						JavaStringFromManagedString(_env, contextConfigStr), 
						JavaStringFromManagedString(_env, serviceConfigStr),
						JavaStringFromManagedString(_env, taskConfigStr));
				}

				IEvaluatorDescriptor^ AllocatedEvaluatorClr2Java::GetEvaluatorDescriptor()
				{
					if(_jobjectAllocatedEvaluator == NULL)
					{
						fprintf(stdout, " _jobjectAllocatedEvaluator is NULL\n"); fflush (stdout);
						return nullptr;
					}
					if(_jmidSubmitContextAndServiceAndTask == NULL)
					{
						fprintf(stdout, " _jmidGetEvaluatorDescriptor is NULL\n"); fflush (stdout);
						return nullptr;
					}
					jstring jevaluatorDescriptorString = (jstring)_env -> CallObjectMethod(
						_jobjectAllocatedEvaluator, 
						_jmidGetEvaluatorDescriptor);
					String^ evaluatorDescriptorString = ManagedStringFromJavaString(_env, jevaluatorDescriptorString);
					EvaluatorDescriptorImpl^ descriptor = gcnew EvaluatorDescriptorImpl(evaluatorDescriptorString);
					return descriptor;
				}
			}
		}
	}
}