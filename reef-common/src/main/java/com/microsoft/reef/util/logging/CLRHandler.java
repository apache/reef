package com.microsoft.reef.util.logging;

import java.util.logging.*;
import java.io.PrintWriter;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import javax.inject.Inject;
import javabridge.*;


public class CLRHandler extends Handler
{
    @Inject
    public CLRHandler()
    {
        super();
    }

    @Override
    public void close() throws SecurityException
    {
    }

    @Override
    public void flush()
    {
    }

    @Override
    public void publish(LogRecord record)
    {
        if (!isLoggable(record))
            return;

        if (record == null)
            return;

        NativeInterop.ClrBufferedLog(3, getFormatter().format(record));
    }
}
