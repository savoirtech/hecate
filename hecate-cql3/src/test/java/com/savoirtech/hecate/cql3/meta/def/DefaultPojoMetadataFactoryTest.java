/*
 * Copyright (c) 2012-2014 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.cql3.meta.def;

import com.savoirtech.hecate.cql3.entities.SimplePojo;
import com.savoirtech.hecate.cql3.meta.PojoMetadata;
import org.junit.Test;

import static org.junit.Assert.assertSame;

public class DefaultPojoMetadataFactoryTest {

    @Test(expected = IllegalArgumentException.class)
    public void testWithNoIdentifier() throws Exception {
        DefaultPojoMetadataFactory factory = new DefaultPojoMetadataFactory();
        factory.getPojoMetadata(NoIdentifier.class);
    }

    @Test
    public void testCaching() {
        DefaultPojoMetadataFactory factory = new DefaultPojoMetadataFactory();
        final PojoMetadata metadata = factory.getPojoMetadata(SimplePojo.class);
        assertSame(metadata, factory.getPojoMetadata(SimplePojo.class));
    }

    public static class NoIdentifier {
        private String name;
    }

}