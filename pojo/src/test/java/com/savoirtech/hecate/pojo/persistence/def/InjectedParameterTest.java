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

package com.savoirtech.hecate.pojo.persistence.def;

import com.google.common.collect.Lists;
import com.savoirtech.hecate.test.AbstractTestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class InjectedParameterTest extends AbstractTestCase {

    @Test
    public void testToString() throws Exception {
        InjectedParameter parameter = new InjectedParameter(2, "foo");
        assertEquals("foo @ 2", parameter.toString());
    }

    @Test
    public void testInjection() {
        final Object injected = new Object();
        InjectedParameter parameter = new InjectedParameter(1, injected);
        List<Object> params = Lists.newLinkedList(Arrays.asList("foo", "bar"));
        parameter.injectInto(params);
        assertEquals(injected, params.get(1));
    }
}