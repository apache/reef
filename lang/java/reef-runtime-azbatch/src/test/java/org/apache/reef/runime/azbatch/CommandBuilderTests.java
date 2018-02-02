package org.apache.reef.runime.azbatch;

import org.apache.reef.runime.azbatch.util.LinuxCommandBuilder;
import org.apache.reef.runime.azbatch.util.WindowsCommandBuilder;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.client.api.JobSubmissionEventImpl;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.Optional;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito.*;

import java.util.Arrays;

import static org.mockito.Mockito.*;

public class CommandBuilderTests {

    private Injector injector;
    private REEFFileNames filenames;
    private LinuxCommandBuilder linuxCommandBuilder;
    private WindowsCommandBuilder windowsCommandBuilder;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws InjectionException {
        this.injector = Tang.Factory.getTang().newInjector();
        this.filenames = this.injector.getInstance(REEFFileNames.class);
        RuntimeClasspathProvider classpathProvider = mock(RuntimeClasspathProvider.class);
        when(classpathProvider.getDriverClasspathPrefix()).thenReturn(Arrays.asList("c:\\driverpath1", "c:\\driverpath2"));
        when(classpathProvider.getEvaluatorClasspathPrefix()).thenReturn(Arrays.asList("c:\\evaluatorpath1", "c:\\evaluatorpath2"));
        when(classpathProvider.getDriverClasspathSuffix()).thenReturn(Arrays.asList("driverclasspathsuffix"));
        when(classpathProvider.getEvaluatorClasspathSuffix()).thenReturn(Arrays.asList("evaluatorclasspathsuffix"));
        this.injector
                .bindVolatileInstance(RuntimeClasspathProvider.class, classpathProvider);
        this.linuxCommandBuilder = this.injector.getInstance(LinuxCommandBuilder.class);
        this.windowsCommandBuilder = this.injector.getInstance(WindowsCommandBuilder.class);

    }

    @Test
    public void LinuxCommandBuilderTest() throws InjectionException {
        JobSubmissionEvent event = mock(JobSubmissionEvent.class);

        Optional<Integer> memory = Optional.of(100);
        when(event.getDriverMemory()).thenReturn(memory);

        String actual = this.linuxCommandBuilder.build(event);
        String expected = "/bin/sh -c \"ln -sf '.' 'reef'; unzip local.jar; java -Xmx100m -XX:PermSize=128m -XX:MaxPermSize=128m -ea -classpath c:\\driverpath1:c:\\driverpath2:reef/local/*:reef/global/*:driverclasspathsuffix -Dproc_reef org.apache.reef.runtime.common.REEFLauncher reef/local/driver.conf 1> stdout.txt 2> stderr.txt\"";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void WindowsCommandBuilderTest() throws InjectionException {
        JobSubmissionEvent event = mock(JobSubmissionEvent.class);

        Optional<Integer> memory = Optional.of(100);
        when(event.getDriverMemory()).thenReturn(memory);

        String actual = this.windowsCommandBuilder.build(event);
        String expected = "powershell.exe /c \"Add-Type -AssemblyName System.IO.Compression.FileSystem;  [System.IO.Compression.ZipFile]::ExtractToDirectory(\\\"$env:AZ_BATCH_TASK_WORKING_DIR\\local.jar\\\", \\\"$env:AZ_BATCH_TASK_WORKING_DIR\\reef\\\");  java -Xmx100m -XX:PermSize=128m -XX:MaxPermSize=128m -ea -classpath 'c:\\driverpath1;c:\\driverpath2;reef/local/*;reef/global/*;driverclasspathsuffix' -Dproc_reef org.apache.reef.runtime.common.REEFLauncher reef/local/driver.conf 1> stdout.txt 2> stderr.txt\";";
        Assert.assertEquals(expected, actual);
    }
}