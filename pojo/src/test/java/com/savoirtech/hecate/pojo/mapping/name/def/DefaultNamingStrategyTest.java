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

package com.savoirtech.hecate.pojo.mapping.name.def;

import com.google.common.base.VerifyException;
import com.savoirtech.hecate.annotation.Column;
import com.savoirtech.hecate.annotation.Index;
import com.savoirtech.hecate.annotation.Table;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.field.FieldFacetProvider;
import com.savoirtech.hecate.pojo.mapping.name.NamingStrategy;
import com.savoirtech.hecate.test.AbstractTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class DefaultNamingStrategyTest extends AbstractTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private NamingStrategy strategy;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private Facet getFacet(Class<?> pojoClass, String fieldName) {
        FieldFacetProvider provider = new FieldFacetProvider();
        Map<String, Facet> map = provider.getFacetsAsMap(pojoClass);
        return map.get(fieldName);
    }

    @Before
    public void init() throws Exception {
        strategy = new DefaultNamingStrategy();
    }

    @Test
    public void testGetAnnotatedColumnName() throws Exception {
        Facet facet = getFacet(AnnotatedPojoType.class, "fieldName");
        DefaultNamingStrategy strategy = new DefaultNamingStrategy();
        assertEquals("Bar", strategy.getColumnName(facet));
    }

    @Test
    public void testGetAnnotatedTableName() throws Exception {
        assertEquals("Foo", strategy.getTableName(AnnotatedPojoType.class));
    }

    @Test
    public void testGetDefaultColumnName() throws Exception {
        Facet facet = getFacet(PojoType.class, "fieldName");
        assertEquals("field_name", strategy.getColumnName(facet));
    }

    @Test
    public void testGetDefaultReferenceTableName() throws Exception {
        assertEquals("child", strategy.getReferenceTableName(getFacet(Referer.class, "noAnnotation")));
    }

    @Test
    public void testGetAnnotatedReferenceTableName() throws Exception {
        assertEquals("children", strategy.getReferenceTableName(getFacet(Referer.class, "withAnnotation")));
    }

    @Test
    public void testGetDefaultTableName() throws Exception {
        assertEquals("pojo_type", strategy.getTableName(PojoType.class));
    }

    @Test
    public void testGetIndexName() {
        assertEquals("default_name_ndx", strategy.getIndexName(getFacet(Indexed.class, "defaultName")));
        assertEquals("foo", strategy.getIndexName(getFacet(Indexed.class, "customName")));
    }

    @Test(expected = VerifyException.class)
    public void testGetIndexNameOnUnindexedField() {
        strategy.getIndexName(getFacet(Referer.class, "withAnnotation"));
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    @Table("Foo")
    private static class AnnotatedPojoType {
        @Column("Bar")
        public String fieldName;
    }

    private static class Child {
        private String stringProperty;
    }

    private static class Parent {
        private Child child;
    }

    private static class PojoType {
        public String fieldName;
    }

    private static class Referer {
        private Child noAnnotation;
        @Table("children")
        private Child withAnnotation;
    }

    private static class Indexed {
        @Index
        private String defaultName;

        @Index("foo")
        private String customName;
    }
}