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

package com.savoirtech.hecate.pojo.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.savoirtech.hecate.core.metrics.HecateMetrics;
import com.savoirtech.hecate.pojo.entities.SimplePojo;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.PojoMappingFactory;
import com.savoirtech.hecate.pojo.mapping.def.DefaultPojoMappingFactory;
import com.savoirtech.hecate.test.AbstractTestCase;
import org.junit.Test;

public class PojoMetricsUtilsTest extends AbstractTestCase {

    @Test
    public void testCreateCounter() throws Exception {
        PojoMappingFactory mappingFactory = new DefaultPojoMappingFactory();
        PojoMapping<SimplePojo> mapping = mappingFactory.createPojoMapping(SimplePojo.class);
        Counter actual = PojoMetricsUtils.createCounter(mapping, "foo");
        Counter expected = HecateMetrics.REGISTRY.counter("SimplePojo.simpletons.foo");
        assertEquals(expected, actual);
    }

    @Test
    public void testCreateTimer() throws Exception {
        PojoMappingFactory mappingFactory = new DefaultPojoMappingFactory();
        PojoMapping<SimplePojo> mapping = mappingFactory.createPojoMapping(SimplePojo.class);
        Timer actual = PojoMetricsUtils.createTimer(mapping, "bar");
        Timer expected = HecateMetrics.REGISTRY.timer("SimplePojo.simpletons.bar");
        assertEquals(expected, actual);
    }

    @Test
    public void testConstructor() {
        assertUtilsClass(PojoMetricsUtils.class);
    }
}