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

package com.savoirtech.hecate.core.metrics;

import com.codahale.metrics.Timer;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

public class MetricsUtilsTest extends Assert {

    @Test
    public void testDoWithTimer() throws Exception {
        Timer timer = HecateMetrics.REGISTRY.timer(RandomStringUtils.randomAlphabetic(10));
        MetricsUtils.doWithTimer(timer, () -> {

        });
        assertEquals(1, timer.getCount());
    }

    @Test
    public void testReturnWithTimer() throws Exception {
        Timer timer = HecateMetrics.REGISTRY.timer(RandomStringUtils.randomAlphabetic(10));
        final String answer = MetricsUtils.returnWithTimer(timer, () -> "foo");
        assertEquals(1, timer.getCount());
        assertEquals("foo", answer);
    }
}