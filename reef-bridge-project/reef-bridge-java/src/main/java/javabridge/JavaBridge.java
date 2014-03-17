package javabridge;
import javabridge.NativeInterop;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

/**
 * Created by beysims on 3/10/14.
 */
    public class JavaBridge {
        private final static String CPP_BRIDGE = "JavaClrBridge";

        static {
            //logger.info("Loading DLL");
            try {
                //System.out.println("before load cppbridge");
                System.loadLibrary(CPP_BRIDGE);
                //System.out.println("after load cppbridge");
                //logger.info("DLL is loaded from memory");
                //loadFromJar();
            } catch (UnsatisfiedLinkError e) {
                //loadFromJar();
            }
        }

    public static Object[] roots = new Object[2];
        public static void main (String[] args){

            InteropReturnInfo interopReturnInfo = new   InteropReturnInfo ();
            InteropLogger interopLogger = new InteropLogger();

            System.out.println("before NativeInterop.createHandler1");
            long handle = NativeInterop.createHandler1 (interopReturnInfo, interopLogger, "hello world");
            byte[] value = new byte[3];
            value[0] = (byte)0xcc;
            value[1] = (byte)0x10;
            value[2] = (byte)0xee;
            NativeInterop.clrHandlerOnNext(handle, value);
            System.out.println("before Exception");
            value[0] = (byte)0x1;
            value[1] = (byte)0x2;
            value[2] = (byte)0x3;
            NativeInterop.clrHandlerOnNext2(interopReturnInfo, interopLogger, handle, value);
            System.out.println("after Exception");
            System.out.println("error code " + interopReturnInfo.getReturnCode());
            String ex = interopReturnInfo.getExceptionList().get(0);
            System.out.println("exception str " + ex);


            value[0] = (byte)0x4;
            value[1] = (byte)0x5;
            value[2] = (byte)0x6;
            NativeInterop.clrHandlerOnNext2(interopReturnInfo, interopLogger, handle, value);
            System.out.println("after Exception");
            System.out.println("error code " + interopReturnInfo.getReturnCode());
            ex = interopReturnInfo.getExceptionList().get(0);
            System.out.println("exception str " + ex);
            ex = interopReturnInfo.getExceptionList().get(1);
            System.out.println("exception str " + ex);


        }

    }

