/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.karaf.itest;

import org.apache.karaf.features.BootFinished;
import org.apache.karaf.features.Feature;
import org.apache.karaf.features.FeaturesService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.karaf.options.LogLevelOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;

import static org.ops4j.pax.exam.CoreOptions.maven;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.*;


@RunWith(PaxExam.class)
public class HecateFeaturesIT extends Assert {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final String DEFAULT_KARAF_VERSION = "3.0.3";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Inject
    private BootFinished bootFinished;

    @Inject
    private FeaturesService featuresService;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected void assertFeatureInstalled(String featureName) {
        try {
            Feature feature = featuresService.getFeature(featureName);
            assertNotNull("Feature " + featureName + " not found!", feature);
            assertTrue(featuresService.isInstalled(feature));
        } catch (Exception e) {
            logger.error("Unable to lookup feature {}!", featureName, e);
            fail();
        }
    }

    @Configuration
    public Option[] configure() {
        final String karafVersion = System.getProperty("karaf.version", DEFAULT_KARAF_VERSION);
        final String projectVersion = System.getProperty("project.version");
        return options(
                karafDistributionConfiguration()
                        .frameworkUrl(maven("org.apache.karaf", "apache-karaf", karafVersion).type("tar.gz"))
                        .unpackDirectory(new File("target/karaf")),
                configureConsole()
                        .startRemoteShell()
                        .ignoreLocalConsole(),
                features(maven("com.savoirtech.hecate", "hecate-karaf", projectVersion).type("xml").classifier("features"), "hecate"),
                logLevel(LogLevelOption.LogLevel.WARN));
    }

    @Test
    public void testBundleInstalled() {
        assertFeatureInstalled("hecate");
    }
}
