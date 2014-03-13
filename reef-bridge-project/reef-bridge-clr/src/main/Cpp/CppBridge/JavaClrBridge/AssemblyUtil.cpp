#include "InteropAssemblies.h"


		void AssemblyUtil::Add(Assembly^  myasm)
		{
			if (0 == _asmCount)
			{
				//asm1 = myasm;
				AppDomain^ currentDomain = AppDomain::CurrentDomain;
				currentDomain->AssemblyResolve += gcnew ResolveEventHandler(&MyResolveEventHandler);
			}
			String^ asmName = myasm->FullName->ToLower();
			Assembly^ existingAsm = nullptr;
			if (!asms2->TryGetValue(asmName, existingAsm))
			{
				Console::WriteLine ("AssemblyUtil:: Adding " + asmName);
				asms2->Add(asmName , myasm);
				++_asmCount;
			}
		}

		Assembly^ AssemblyUtil::FindAsm (String^ myasm)
		{
			Assembly^ returnAsm = nullptr;
			if (!asms2->TryGetValue(myasm->ToLower(),returnAsm))
			{
				//ManagedLog ("FindAsm_Not_Found", myasm);
				Console::WriteLine ("AssemblyUtil:: FindAsm_Not_Found " + myasm->ToString());
			}
			return returnAsm;
		}

		Assembly^ AssemblyUtil::MyResolveEventHandler(Object^ sender, ResolveEventArgs^ args)
			{		
				//ManagedLog ("lResolving", args->Name);	
				Console::WriteLine ("AssemblyUtil:: Resolving " + args->Name);	
				Assembly^ myAsm = AssemblyUtil::FindAsm(args->Name);
				if (nullptr != myAsm)
				{
					Console::WriteLine ("AssemblyUtil:: Found " + args->Name);	
				}
				return myAsm ;
			}
  	
