package javabridge;
import  java.util.ArrayList;

/**
 * Created by beysims on 3/14/14.
 */
public class InteropReturnInfo {

    int returnCode;
    ArrayList<String> exceptionList = new ArrayList<String>();

    public void setReturnCode(int rc)
    {
        returnCode = rc;
    }

    public void addExceptionString (String exceptionString)
    {
        exceptionList.add(exceptionString);
    }

    public boolean hasExceptions ()
    {
        return !exceptionList.isEmpty();
    }

    public ArrayList<String> getExceptionList()
    {
        return exceptionList;
    }


    public int getReturnCode()
    {
        return returnCode;
    }

    public void reset()
    {
        exceptionList = new ArrayList<String>();
        returnCode = 0;
    }
}
