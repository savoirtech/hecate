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

package com.savoirtech.hecate.pojo.util;

import com.savoirtech.hecate.annotation.Column;
import com.savoirtech.hecate.annotation.Table;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.SubFacet;
import com.savoirtech.hecate.pojo.facet.field.FieldFacet;
import org.junit.Assert;
import org.junit.Test;

public class PojoUtilsTest extends Assert {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testGetColumnName() throws ReflectiveOperationException {
        assertEquals("field_name", PojoUtils.getColumnName(fieldOf(PojoType.class, "fieldName")));
        assertEquals("Bar", PojoUtils.getColumnName(fieldOf(AnnotatedPojoType.class, "fieldName")));

        assertEquals("child_string_property", PojoUtils.getColumnName(new SubFacet(fieldOf(Parent.class, "child"), fieldOf(Child.class, "stringProperty"), true)));
    }

    private Facet fieldOf(Class<?> type, String name) throws ReflectiveOperationException {
        return new FieldFacet(type, type.getDeclaredField(name));
    }

    @Test
    public void testGetTableName() {
        assertEquals("pojo_type", PojoUtils.getTableName(PojoType.class));
        assertEquals("Foo", PojoUtils.getTableName(AnnotatedPojoType.class));
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
}