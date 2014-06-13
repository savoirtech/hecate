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

package com.savoirtech.hecate.cql3.value.field;

import java.util.List;
import java.util.Map;

import com.savoirtech.hecate.cql3.annotations.Id;
import com.savoirtech.hecate.cql3.annotations.Transient;
import com.savoirtech.hecate.cql3.value.Facet;
import org.junit.Test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class FieldFacetProviderTest {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testGet() throws Exception {
        FieldFacet value = fooField();
        final FieldHolder holder = new FieldHolder();
        assertEquals("bar", value.get(holder));
    }

    private FieldFacet fooField() throws Exception {
        return new FieldFacet(FieldHolder.class, FieldHolder.class.getDeclaredField("foo"));
    }

    @Test
    public void testGetAnnotation() throws Exception {
        FieldFacet value = fooField();
        assertNotNull(value.getAnnotation(Id.class));
    }

    @Test
    public void testGetName() throws Exception {
        FieldFacet value = fooField();
        assertEquals("foo", value.getName());
    }

    @Test
    public void testGetType() throws Exception {
        FieldFacet value = fooField();
        assertEquals(String.class, value.getType().getRawType());
    }

    @Test
    public void testSet() throws Exception {
        FieldFacet value = fooField();
        final FieldHolder holder = new FieldHolder();
        value.set(holder, "baz");
        assertEquals("baz", holder.foo);
    }

    @Test
    public void testWithNonPersistentFields() {
        FieldFacetProvider provider = new FieldFacetProvider();
        List<Facet> facets = provider.getFacets(NonPersistentFields.class);
        assertEquals(0, facets.size());
    }

    @Test
    public void testWithPersistentFields() {
        FieldFacetProvider provider = new FieldFacetProvider();
        final List<Facet> facets = provider.getFacets(PersistentFields.class);
        assertEquals(1, facets.size());
    }

    @Test
    public void testWithTransientFields() {
        FieldFacetProvider provider = new FieldFacetProvider();
        final List<Facet> facets = provider.getFacets(TransientFields.class);
        assertEquals(1, facets.size());
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private static class FieldHolder {
        @Id
        private String foo = "bar";

        private Map<String, String> map;
    }

    public static class NonPersistentFields {
        private static String staticField = "static";
        private transient String transientField = "transient";
        private final String finalField = "final";
    }

    private static class PersistentFields {
        private String foo = "hello";
    }

    private static class TransientFields {

        private String foo = "hello";

        @Transient
        private String bar = "bye";
    }
}