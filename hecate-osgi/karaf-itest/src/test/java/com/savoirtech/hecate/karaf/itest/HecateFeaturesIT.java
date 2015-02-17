/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.karaf.itest;

import com.savoirtech.hecate.osgi.example.PersonRepository;
import org.apache.felix.service.command.CommandProcessor;
import org.apache.felix.service.command.CommandSession;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.ProbeBuilder;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.karaf.options.LogLevelOption;
import org.osgi.framework.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.ops4j.pax.exam.CoreOptions.*;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.*;

@RunWith(PaxExam.class)
public class HecateFeaturesIT extends Assert {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final Long COMMAND_TIMEOUT = 600000L;

    @Inject
    private PersonRepository personRepository;

    @Inject
    private CommandProcessor commandProcessor;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    protected void assertBundleActive(String symbolicName) {
        final String message = String.format("Bundle %s is not active!", symbolicName);
        final String command = String.format("bundle:list -t 10 -s | grep '%s'", symbolicName);
        assertCommandOutputContains(message, command, "Active");
    }

    /**
     * Executes a shell command and returns output as a String.
     * Commands have a default timeout of 10 seconds.
     *
     * @param command the command
     * @return the command output
     */
    protected String executeCommand(final String command) {
        return executeCommand(command, COMMAND_TIMEOUT);
    }

    /**
     * Executes a shell command and returns output as a String.
     * Commands have a default timeout of 10 seconds.
     *
     * @param command The command to onExecute.
     * @param timeout The amount of time in millis to wait for the command to onExecute.
     * @return command output
     */
    protected String executeCommand(final String command, final Long timeout) {
        String response;

        FutureTask<String> commandFuture = new FutureTask<>(new Callable<String>() {
            public String call() {
                try {
                    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    final PrintStream printStream = new PrintStream(byteArrayOutputStream, false, "UTF-8");
                    final CommandSession commandSession = commandProcessor.createSession(System.in, printStream, System.err);
                    commandSession.execute(command);
                    printStream.flush();
                    return byteArrayOutputStream.toString("UTF-8");
                } catch (Exception e) {
                    logger.error("Command threw exception!", e);
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            executor.submit(commandFuture);
            response = commandFuture.get(timeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("An exception occurred while executing command {}!", command, e);
            response = "SHELL COMMAND TIMED OUT: ";
        }

        return response;
    }

    protected void assertCommandOutputContains(String message, String command, String pattern) {
        final String output = executeCommand(command);
        System.out.println();
        System.out.printf("karaf> %s%n", command);
        System.out.println();
        System.out.println(output);

        assertTrue(message, output.contains(pattern));
    }

    @Configuration
    public final Option[] config() {
        final String projectVersion = System.getProperty("project.version");
        return options(
                junitBundles(),
                karafDistributionConfiguration()
                        .frameworkUrl(maven().groupId("org.apache.karaf").artifactId("apache-karaf").type("tar.gz").version("3.0.3"))
                        .unpackDirectory(new File("target/karaf")),
                systemTimeout(120000),
                keepRuntimeFolder(),
                features(maven().groupId("com.savoirtech.hecate.karaf").artifactId("hecate").version(projectVersion).classifier("features").type("xml"), "hecate", "hecate-osgi-example"),
                configureConsole().ignoreLocalConsole(),
                logLevel(LogLevelOption.LogLevel.INFO));
    }

    @ProbeBuilder
    public TestProbeBuilder probeConfiguration(TestProbeBuilder probe) {
        probe.setHeader(Constants.DYNAMICIMPORT_PACKAGE, "*");
        return probe;
    }

    @Test
    public void testHecateFeature() {
        // Do nothing (injecting repository verifies)
        assertBundleActive("com.savoirtech.hecate.cql3");
        assertBundleActive("com.savoirtech.hecate.osgi.example");

        assertNotNull(personRepository.findBySsn("123456789"));
    }
}
