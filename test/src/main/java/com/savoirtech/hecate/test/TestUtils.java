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
package com.savoirtech.hecate.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import org.junit.Before;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtils {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    protected final Logger logger = LoggerFactory.getLogger(getClass());

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public static void assertUtilsClass(Class<?> c) {
        Constructor ctor = null;
        try {
            ctor = c.getDeclaredConstructor();
            assertTrue(Modifier.isPrivate(ctor.getModifiers()));
            assertInstantiatable(ctor);
        } catch (NoSuchMethodException e) {
            fail("No default constructor defined for class " + c.getCanonicalName());
        }
    }

    private static void assertInstantiatable(Constructor ctor) {
        ctor.setAccessible(true);
        try {
            ctor.newInstance();
        } catch (ReflectiveOperationException e) {
            fail("Unable to instantiate!");
        }
    }

    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
    }
}
