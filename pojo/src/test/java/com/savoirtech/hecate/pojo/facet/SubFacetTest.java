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

package com.savoirtech.hecate.pojo.facet;

import com.savoirtech.hecate.annotation.Column;
import com.savoirtech.hecate.pojo.facet.field.FieldFacetProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class SubFacetTest extends Assert {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private SubFacet strictFacet;
    private SubFacet lenientFacet;
    private Facet childFacet;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void setUpFacet() {
        FacetProvider facetProvider = new FieldFacetProvider();
        final Map<String, Facet> parentFacets = facetProvider.getFacetsAsMap(Parent.class);
        final Map<String, Facet> childFacets = facetProvider.getFacetsAsMap(Child.class);
        childFacet = childFacets.get("property");
        Facet parentFacet = parentFacets.get("child");
        strictFacet = new SubFacet(parentFacet, childFacet, false);
        lenientFacet = new SubFacet(parentFacet, childFacet, true);
    }

    @Test
    public void testGetAnnotation() {
        Column column = strictFacet.getAnnotation(Column.class);
        assertNotNull(column);
        assertEquals("foo", column.value());
    }

    @Test
    public void testGetName() {
        assertEquals("child.property", strictFacet.getName());
    }

    @Test
    public void testGetType() {
        assertSame(childFacet.getType(), strictFacet.getType());
    }

    @Test
    public void testLenientGetValue() {
        Parent parent = new Parent();
        assertNull(lenientFacet.getValue(parent));
        parent.child = new Child();
        assertNull(lenientFacet.getValue(parent));
        parent.child.property = "test_value";
        assertEquals("test_value", lenientFacet.getValue(parent));
    }

    @Test
    public void testLenientSetValue() {
        Parent parent = new Parent();
        lenientFacet.setValue(parent, "test_value");
        assertNotNull(parent.child);
        lenientFacet.setValue(parent, "test_value");
        assertEquals("test_value", parent.child.property);
    }

    @Test
    public void testStrictGetValue() {
        Parent parent = new Parent();
        assertNull(strictFacet.getValue(parent));
        parent.child = new Child();
        assertNull(strictFacet.getValue(parent));
    }

    @Test
    public void testStrictSetValue() {
        Parent parent = new Parent();
        strictFacet.setValue(parent, "test_value");
        assertNull(parent.child);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class Child {
        @Column("foo")
        private String property;
    }

    public static class Parent {
        private Child child;
    }
}