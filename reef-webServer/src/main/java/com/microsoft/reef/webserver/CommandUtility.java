package com.microsoft.reef.webserver;

import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

/**
 * utility to run command
 */
public class CommandUtility {
    public static byte[] runCommand(String command) {
        final StringBuilder sb = new StringBuilder();
        try {
            // Execute the command
            String cmd = isWindows()  ? "cmd.exe /c " + command : command;
            final Process proc = Runtime.getRuntime().exec(cmd);
            final BufferedReader input =
                    new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String line;
            while ((line = input.readLine()) != null) {
                sb.append(line).append('\n');
            }
        } catch (IOException ex) {
            sb.append(ex);
        }
        return sb.toString().getBytes(Charset.forName("UTF-8"));
    }

    public static boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().contains("windows");
    }
}
