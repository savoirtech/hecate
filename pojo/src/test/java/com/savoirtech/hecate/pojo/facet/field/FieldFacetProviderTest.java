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

package com.savoirtech.hecate.pojo.facet.field;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.savoirtech.hecate.annotation.Cascade;
import com.savoirtech.hecate.annotation.Ignored;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.entities.NestedPojo;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.FacetProvider;
import org.junit.Assert;
import org.junit.Test;

public class FieldFacetProviderTest extends Assert {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testCascadingRules() {
        FacetProvider facetProvider = new FieldFacetProvider();
        Map<String, Facet> map = facetProvider.getFacetsAsMap(CascadeMe.class);

        assertTrue(map.get("nestedAllCascade").isCascadeDelete());
        assertTrue(map.get("nestedAllCascade").isCascadeSave());

        assertFalse(map.get("nestedNoCascade").isCascadeDelete());
        assertFalse(map.get("nestedNoCascade").isCascadeSave());

        assertTrue(map.get("nestedDeleteOnly").isCascadeDelete());
        assertFalse(map.get("nestedDeleteOnly").isCascadeSave());

        assertFalse(map.get("nestedSaveOnly").isCascadeDelete());
        assertTrue(map.get("nestedSaveOnly").isCascadeSave());
    }

    @Test
    public void testGetAnnotation() {
        FacetProvider facetProvider = new FieldFacetProvider();
        Map<String, Facet> map = facetProvider.getFacetsAsMap(FieldPojoSub.class);
        Facet facet = map.get("baz");
        assertNotNull(facet.getAnnotation(Deprecated.class));
    }

    @Test
    public void testGetFacets() throws Exception {
        FacetProvider facetProvider = new FieldFacetProvider();
        List<Facet> facets = facetProvider.getFacets(FieldPojoSub.class);
        Set<String> names = facets.stream().map(Facet::getName).collect(Collectors.toSet());
        assertTrue(names.contains("foo"));
        assertTrue(names.contains("bar"));
        assertTrue(names.contains("baz"));
        assertFalse(names.contains("ignoredTransient"));
        assertFalse(names.contains("ignoredAnnotated"));
    }

    @Test
    public void testGetType() {
        FacetProvider facetProvider = new FieldFacetProvider();
        Map<String, Facet> map = facetProvider.getFacetsAsMap(FieldPojoSub.class);
        Facet facet = map.get("bar");
        assertEquals(String.class, facet.getType().getRawType());
    }

    @Test
    public void testGetValue() {
        FacetProvider facetProvider = new FieldFacetProvider();
        Map<String, Facet> map = facetProvider.getFacetsAsMap(FieldPojoSub.class);
        Facet facet = map.get("bar");
        FieldPojoSub pojo = new FieldPojoSub();
        assertNull(facet.getValue(pojo));
        facet.setValue(pojo, "testValue");
        assertEquals("testValue", facet.getValue(pojo));
    }

    @Test
    public void testSettingFinalField() {
        FacetProvider facetProvider = new FieldFacetProvider();
        Map<String, Facet> map = facetProvider.getFacetsAsMap(FieldPojoSub.class);
        Facet foo = map.get("foo");
        FieldPojoSub pojo = new FieldPojoSub();
        foo.setValue(pojo, "bar");
        assertEquals("bar", foo.getValue(pojo));
    }

    @Test
    public void testToString() {
        FacetProvider facetProvider = new FieldFacetProvider();
        Map<String, Facet> map = facetProvider.getFacetsAsMap(FieldPojoSub.class);
        Facet foo = map.get("foo");
        assertEquals("foo", foo.toString());
    }

    @Test
    public void testFlatten() {
        FacetProvider facetProvider = new FieldFacetProvider();
        Map<String, Facet> map = facetProvider.getFacetsAsMap(FieldPojoSub.class);
        Facet foo = map.get("foo");
        assertEquals(foo, foo.flatten());
    }

    @Test(expected = HecateException.class)
    public void testDuplicateFacetsInMap() {
        FacetProvider facetProvider = new FieldFacetProvider();
        facetProvider.getFacetsAsMap(DuplicateField.class);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class CascadeMe {
        @Cascade(save = false)
        private NestedPojo nestedDeleteOnly;

        @Cascade(delete = false)
        private NestedPojo nestedSaveOnly;

        @Cascade(delete = false, save = false)
        private NestedPojo nestedNoCascade;

        private NestedPojo nestedAllCascade;
    }

    public static class FieldPojoSub extends FieldPojoSuper {
        private static String STATIC_TEXT = "static_text";
        @Deprecated
        private String baz;
        private transient String ignoredTransient;

        @Ignored
        private String ignoredAnnotated;
    }

    public static class FieldPojoSuper {
        private final String foo = "foo";
        private String bar;
    }

    public static class DuplicateField extends FieldPojoSuper {
        private String bar;
    }
}