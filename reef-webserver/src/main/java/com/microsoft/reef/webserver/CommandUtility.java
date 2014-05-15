package com.microsoft.reef.webserver;

import com.microsoft.reef.util.OSUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

/**
 * utility to run command
 */
public class CommandUtility {
    public static String runCommand(final String command) {
        final StringBuilder sb = new StringBuilder();
        try {
            String cmd = OSUtils.isWindows() ? "cmd.exe /c " + command : command;
            final Process proc = Runtime.getRuntime().exec(cmd);

            try (final BufferedReader input =
                         new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
                String line;
                while ((line = input.readLine()) != null) {
                    sb.append(line).append('\n');
                }
            }
        } catch (IOException ex) {
            sb.append(ex);
        }
        return sb.toString();
    }
}
