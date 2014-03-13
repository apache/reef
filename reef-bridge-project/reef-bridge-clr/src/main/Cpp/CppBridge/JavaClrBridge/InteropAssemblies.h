#pragma once
#define _USING_V110_SDK71_

#pragma warning( push )
#pragma warning( disable : 4793 )
#include <jni.h>
#pragma warning( pop )
#include "mscoree.h"
#include "vcclr.h"

using namespace System;
using namespace System::Reflection;
using namespace System::Collections::Generic;

public ref class AssemblyUtil
	{
	public :
		static int _asmCount = 0;
		static Dictionary<String^, System::Reflection::Assembly^>^  asms2 = gcnew Dictionary<String^, Assembly^>();
		static void Add(Assembly^  myasm);		
		static Assembly^ FindAsm (String^ myasm);
		static Assembly^ MyResolveEventHandler(Object^ sender, ResolveEventArgs^ args);  	
	};
