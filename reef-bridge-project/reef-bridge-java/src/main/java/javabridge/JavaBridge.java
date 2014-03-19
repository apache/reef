package javabridge;
import javabridge.NativeInterop;

import java.util.Date;

/**
 * Created by beysims on 3/10/14.
 */
    public class JavaBridge {
        private final static String CPP_BRIDGE = "JavaClrBridge";

        static {
            //logger.info("Loading DLL");
            try {
                System.out.println("before load cppbridge");
                System.loadLibrary(CPP_BRIDGE);
                System.out.println("after load cppbridge");
                //logger.info("DLL is loaded from memory");
                //loadFromJar();
            } catch (UnsatisfiedLinkError e) {
                //loadFromJar();
            }
        }

        public static void main (String[] args){
            String strDate = new Date().toString();
            System.out.println("java side date " + strDate);
            long handle0 = NativeInterop.CallClrSystemOnStartHandler(new Date().toString());
            byte[] value = new byte[3];
            value[0] = (byte)0xcc;
            value[1] = (byte)0x10;
            value[2] = (byte)0xee;
            EManager eManager = new EManager();
            InteropLogger interopLogger = new InteropLogger();
            NativeInterop.CallClrSystemAllocatedEvaluatorHandlerOnNext(handle0, eManager, interopLogger, value);


        }
    public static void oldMain(){
        //NativeInterop.loadClrAssembly("d:\\yingda\\CSharp\\ClrHandler\\bin\\Debug\\ClrHandler.dll");
        String strDate = new Date().toString();
        System.out.println("java side date " + strDate);
        long handle0 = NativeInterop.CallClrSystemOnStartHandler(new Date().toString());
        System.out.println("before NativeInterop.createHandler1");
        long handle = NativeInterop.createHandler1 ("hello yingda0");
        byte[] value = new byte[3];
        value[0] = (byte)0xcc;
        value[1] = (byte)0x10;
        value[2] = (byte)0xee;
        //NativeInterop.CallClrSystemAllocatedEvaluatorHandlerOnNext(handle0, value);
        NativeInterop.clrHandlerOnNext(handle, value);
        System.out.println("before Exception");
        value[0] = (byte)0x1;
        value[1] = (byte)0x2;
        value[2] = (byte)0x3;
        InteropReturnInfo ret = new   InteropReturnInfo ();
        NativeInterop.clrHandlerOnNext2(ret, handle, value);
        System.out.println("after Exception");
        System.out.println("error code " + ret.getReturnCode());
        String ex = ret.getExceptionList().get(0);
        System.out.println("exception str " + ex);

    }


    }

