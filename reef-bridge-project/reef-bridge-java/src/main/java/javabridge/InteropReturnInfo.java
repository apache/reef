package javabridge;
import  java.util.ArrayList;

/**
 * Created by beysims on 3/14/14.
 */
public class InteropReturnInfo {

    int returnCode;
    ArrayList<String> exceptionList = new ArrayList<String>();

    public void AddExceptionString (String exceptionString)
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

    public void SetReturnCode(int rc)
    {
        returnCode = rc;
    }

    public int getReturnCode()
    {
        return returnCode;
    }
}
