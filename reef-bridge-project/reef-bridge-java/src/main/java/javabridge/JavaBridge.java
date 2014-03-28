package javabridge;
import javabridge.NativeInterop;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.util.Date;

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

            String strDate = new Date().toString();
            System.out.println("java side date " + strDate);
            long handle0 = NativeInterop.CallClrSystemOnStartHandler(new Date().toString());

            InteropLogger interopLogger = new InteropLogger();

            byte[] value = new byte[3];
            value[0] = (byte)0xcc;
            value[1] = (byte)0x10;
            value[2] = (byte)0xee;

            EManager eManager = new EManager();
            DriverManager driverManager = new DriverManager();
            NativeInterop.CallClrSystemAllocatedEvaluatorHandlerOnNext(handle0, eManager, driverManager, interopLogger, value);




        }
    }

