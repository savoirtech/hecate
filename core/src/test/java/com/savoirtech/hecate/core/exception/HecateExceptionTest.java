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

package com.savoirtech.hecate.core.exception;

import org.junit.Assert;
import org.junit.Test;

public class HecateExceptionTest extends Assert {

    @Test
    public void testMessageInterpolation() {
        HecateException e = new HecateException("%s - %s", "foo", "bar");
        assertEquals("foo - bar", e.getMessage());
    }

    @Test
    public void testWithNestedException() {
        Exception nested = new IllegalArgumentException("Oops");
        HecateException e = new HecateException(nested, "%s - %s", "foo", "bar");
        assertEquals("foo - bar", e.getMessage());
        assertEquals(nested, e.getCause());
    }
}