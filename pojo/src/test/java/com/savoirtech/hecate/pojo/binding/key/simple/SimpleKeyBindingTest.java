/*
 * Copyright (c) 2012-2016 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.pojo.binding.key.simple;

import java.util.Collections;
import java.util.List;

import com.datastax.driver.core.DataType;
import com.google.common.collect.Lists;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.pojo.binding.PojoVisitor;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.test.BindingTestCase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SimpleKeyBindingTest extends BindingTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private SimpleKeyBinding binding;

    @Mock
    private PojoVisitor visitor;

    @Mock
    private Converter converter;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void setUp() throws Exception {
        binding = new SimpleKeyBinding(getFacet(SimpleEntity.class, "id"), "id", converter);
    }

    @Test
    public void testCreate() {
        when(converter.getDataType()).thenReturn(DataType.varchar());
        assertCreateEquals(binding, "CREATE TABLE foo( id varchar, PRIMARY KEY(id))");
        verify(converter).getDataType();
    }

    @Test
    public void testDelete() {
        assertDeleteEquals(binding, "DELETE FROM foo WHERE id=?;");
    }

    @Test
    public void testInsert() {
        assertInsertEquals(binding, "INSERT INTO foo(id) VALUES (?);");
    }

    @Test
    public void testSelect() {
        assertSelectEquals(binding, "SELECT id FROM foo;");
    }

    @Test
    public void testSelectWhere() {
        assertSelectWhereEquals(binding, "SELECT id FROM foo WHERE id=?;");
    }

    @Test
    public void testVisitChildren() {
        binding.visitFacetChildren("foo", facet -> true, visitor);
        Mockito.verifyNoMoreInteractions(visitor);
    }

    @Test
    public void testGetElementDataType() {
        when(converter.getDataType()).thenReturn(DataType.varchar());
        assertEquals(DataType.varchar(), binding.getElementDataType());
    }

    @Test
    public void testGetElementType() {
        when(converter.getValueType()).thenAnswer((Answer<Object>) invocationOnMock -> String.class);
        assertEquals(String.class, binding.getElementType());
    }

    @Test
    public void testElementToKeys() {
        assertEquals(Collections.singletonList("foo"), binding.elementToKeys("foo"));
    }

    @Test
    public void testGetElementValue() {
        when(converter.toColumnValue("1")).thenReturn("2");
        SimpleEntity entity = new SimpleEntity();
        entity.setId("1");
        assertEquals("2", binding.getElementValue(entity));
        verify(converter).toColumnValue("1");
    }

    @Test
    public void testGetKeyParameters() {
        when(converter.toColumnValue("1")).thenReturn("2");
        List<Object> keys = binding.getKeyParameters(Lists.newArrayList("1"));
        assertEquals(Lists.newArrayList("2"), keys);
        verify(converter).toColumnValue("1");
    }

    public static class SimpleEntity {
        @PartitionKey
        private String id;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }
}